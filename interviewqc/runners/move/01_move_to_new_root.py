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
import shutil

from rich.logging import RichHandler

from interviewqc.helpers import utils, db
from interviewqc.models.moved_file import MovedFile

MODULE_NAME = "interviewqc_move_to_new_root"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_new_path(old_path: Path, data_root: Path, backup_root: Path) -> Path:
    """
    Gets the new path for a file.

    Args:
        old_path (Path): The old path of the file.
        backup_root (Path): The new root directory.
        data_root (Path): The old root directory.

    Returns:
        Path: The new path of the file.
    """
    relative_path = old_path.relative_to(data_root)
    new_path = backup_root / relative_path
    return new_path


def log_move(config_file: Path, old_path: Path, new_path: Path):
    """
    Logs a move to the database.

    Args:
        config_file (Path): The path to the configuration file.
        old_path (Path): The old path of the file.
        new_path (Path): The new path of the file.
    """

    moved_file = MovedFile(
        source_file_path=str(old_path), destination_file_path=str(new_path)
    )

    query = moved_file.to_sql()
    db.execute_queries(
        config_file=config_file, queries=[query], show_commands=False, silent=True
    )


def move_subject(
    config_file: Path, subject_dir: Path, data_root: Path, backup_root: Path
):
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

        for file_path in interview_files:
            new_path = get_new_path(
                old_path=file_path, data_root=data_root, backup_root=backup_root
            )

            if not new_path.parent.exists():
                new_path.parent.mkdir(parents=True, exist_ok=True)

            logger.debug(f"Moving {file_path} to {new_path}")
            shutil.move(src=file_path, dst=new_path)
            log_move(config_file=config_file, old_path=file_path, new_path=new_path)

        shutil.rmtree(path=interview_type_dir)
        # recreate the directory (empty)
        interview_type_dir.mkdir()


def move_site(
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
            move_subject(
                config_file=config_file,
                subject_dir=subject_dir,
                data_root=data_root,
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
        move_site(
            config_file=config_file,
            site_name=site,
            network=network,
            data_root=data_root,
            backup_root=backup_root,
        )

    logger.info("Done")
