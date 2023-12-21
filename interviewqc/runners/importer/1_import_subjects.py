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
from typing import List
from datetime import datetime

from rich.logging import RichHandler
import pandas as pd

from interviewqc.helpers import utils, db
from interviewqc.helpers.config import config
from interviewqc.models.subject import Subject


MODULE_NAME = "interviewqc_import_subjects"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subjects_from_metadata(metadata_file: Path) -> List[Subject]:
    """
    Retrieves a list of subjects from a metadata file.

    Args:
        metadata_file (Path): The path to the metadata file.

    Returns:
        List[Subject]: A list of Subject objects.

    """
    subjects: List[Subject] = []

    metadata_df = pd.read_csv(metadata_file)
    for _, row in metadata_df.iterrows():
        subject_id = row["Subject ID"]
        site_id = subject_id[:2]
        consent_date = datetime.strptime(row["Consent"], "%Y-%m-%d")

        subject = Subject(
            subject_id=subject_id,
            site_id=site_id,
            consent_date=consent_date,
        )

        subjects.append(subject)

    return subjects


def get_all_subjects(config_file: Path, data_root: Path):
    """
    Retrieves all subjects from the specified data root directory.

    Args:
        config_file (Path): The path to the configuration file.
        data_root (Path): The root directory where the subject data is stored.

    Returns:
        None
    """
    sites_path = data_root / "GENERAL"
    subjects: List[Subject] = []

    for site_path in sites_path.iterdir():
        if not site_path.is_dir():
            continue

        site_name = site_path.name

        metadata_file = site_path / f"{site_name}_metadata.csv"
        if not metadata_file.exists():
            logger.warning(f"Metadata file not found: {metadata_file}")
            continue

        logger.info(f"Getting subjects from {metadata_file}")
        subjects += get_subjects_from_metadata(metadata_file)

    logger.info(f"Got {len(subjects)} subjects")

    sql_queries: List[str] = []
    for subject in subjects:
        query = subject.to_sql()
        sql_queries.append(query)

    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = config(config_file, "general")
    data_root = Path(config_params["data_root"])
    logger.info(f"Data root: {data_root}")

    logger.info("Getting all subjects")
    get_all_subjects(config_file=config_file, data_root=data_root)

    logger.info("Done")
