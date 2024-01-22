#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-interview-qc":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass


import logging
from argparse import ArgumentParser
from typing import List
import os

from rich.logging import RichHandler

from interviewqc.helpers import utils, db, hash, cli

MODULE_NAME = "interviewqc_move_remove_duplicates"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_moved_files_by_hash(config_file: Path, md5_hash: str) -> List[Path]:
    """
    Get all the files that have been moved with the same hash.

    Args:
        config_file (Path): The path to the config file.
        md5_hash (str): The hash to search for.

    Returns:
        List[Path]: A list of paths to the files that have been moved with the same hash.
    """

    query = f"""
        SELECT destination_file_path FROM moved_files
        WHERE md5 = '{md5_hash}';
    """

    moved_files_df = db.execute_sql(config_file=config_file, query=query)

    moved_files: List[Path] = []
    for _, row in moved_files_df.iterrows():
        moved_files.append(Path(row["destination_file_path"]))

    return moved_files


def clear_subject(config_file: Path, subject_dir: Path, backup_root: Path):
    interviews_dir = subject_dir / "interviews"

    interview_types: List[str] = ["open", "psychs"]

    for interview_type in interview_types:
        interview_type_dir = interviews_dir / interview_type

        if not interview_type_dir.exists():
            continue

        interview_files: List[Path] = []
        for root, dirs, files in os.walk(interview_type_dir):
            for file in files:
                file_path = Path(root) / file
                interview_files.append(file_path)

        for interview_file in interview_files:
            md5_hash = hash.compute_hash(interview_file)
            moved_files = get_moved_files_by_hash(
                config_file=config_file, md5_hash=md5_hash
            )

            if len(moved_files) > 1:
                cli.remove_files(files=moved_files, base_dir=backup_root, logger=logger)


def clear_site(
    config_file: Path, site_name: str, network: str, data_root: Path, backup_root: Path
):
    logger.info(f"Moving site {site_name}")
    site_path = data_root / "PROTECTED" / f"{network}{site_name}" / "raw"

    if not site_path.exists():
        logger.error(f"Site path {site_path} does not exist")
        raise FileNotFoundError(f"Site path {site_path} does not exist")

    subjects_dir = site_path.iterdir()
    subjects_dir_list: List[Path] = [subject_dir for subject_dir in subjects_dir]

    with utils.get_progress_bar() as progress_bar:
        task = progress_bar.add_task(
            f"Moving {site_name}", total=len(subjects_dir_list)
        )
        for subject_dir in subjects_dir_list:
            progress_bar.update(task, advance=1, description=subject_dir.name)
            clear_subject(
                config_file=config_file,
                subject_dir=subject_dir,
                backup_root=backup_root,
            )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])
    network = config_params["network"]
    logger.info(f"Data root: {data_root}")
    logger.info(f"Network: {network}")

    config_params = utils.config(config_file, "move")
    backup_root = Path(config_params["backup_root"])
    logger.info(f"Backup root: {backup_root}")

    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "--sites",
        dest="sites",
        nargs="+",
        required=True,
        help="The sites to move to the new root.",
    )

    args = arg_parser.parse_args()

    sites = args.sites
    logger.info(f"Sites: {sites}")

    for site in sites:
        clear_site(
            config_file=config_file,
            site_name=site,
            network=network,
            data_root=data_root,
            backup_root=backup_root,
        )

    logger.info("Done")
