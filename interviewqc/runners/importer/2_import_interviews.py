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

from interviewqc.helpers import utils, db
from interviewqc.helpers.config import config
from interviewqc.models.interview import Interview


MODULE_NAME = "interviewqc_import_interviews"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


INVALID_INTERVIEW_NAMES_COUNT = 0


def get_interviews_from_dir(
    interviews_dir: Path, subject_id: str, interview_type: str
) -> List[Interview]:
    """
    Retrieves a list of Interview objects from the specified directory.

    Args:
        interviews_dir (Path): The directory containing the interviews.
        subject_id (str): The ID of the subject.
        interview_type (str): The type of the interview.

    Returns:
        List[Interview]: A list of Interview objects.

    """
    interviews: List[Interview] = []
    global INVALID_INTERVIEW_NAMES_COUNT

    for interview_dir in interviews_dir.iterdir():
        if not interview_dir.is_dir():
            continue

        interview_name = interview_dir.name
        valid_name = True
        try:
            interview_datetime = interview_name[:19]
            interview_date = datetime.strptime(interview_datetime, "%Y-%m-%d %H.%M.%S")
        except ValueError as e:
            if (
                not interview_name == "Audio Record"
                and not interview_name == "for review"
            ):
                logger.warning(f"Interview '{interview_dir}' has Out-of-SOP name")
                interview_date = None
                valid_name = False
                INVALID_INTERVIEW_NAMES_COUNT += 1
            else:
                continue

        interview = Interview(
            interview_path=interview_dir,
            interview_name=interview_name,
            interview_type=interview_type,
            subject_id=subject_id,
            interview_date=interview_date,
            valid_name=valid_name,
        )

        interviews.append(interview)

    return interviews


def get_interviews_from_subject(subject_path: Path) -> List[Interview]:
    """
    Retrieves a list of interviews from the specified subject path.

    Args:
        subject_path (Path): The path to the subject directory.

    Returns:
        List[Interview]: A list of Interview objects.

    """
    interview_path = subject_path / "interviews"
    subject_id = subject_path.name

    interviews: List[Interview] = []

    # open Interviews
    open_interviews_path = interview_path / "open"

    if open_interviews_path.exists():
        interviews += get_interviews_from_dir(open_interviews_path, subject_id, "open")

    # psychs Interviews
    psychs_interviews_path = interview_path / "psychs"

    if psychs_interviews_path.exists():
        interviews += get_interviews_from_dir(
            psychs_interviews_path, subject_id, "psychs"
        )

    return interviews


def get_interviews_from_site(site_path: Path) -> List[Interview]:
    """
    Retrieves a list of interviews from the specified site path.

    Args:
        site_path (Path): The path to the site directory.

    Returns:
        List[Interview]: A list of Interview objects.

    Raises:
        FileNotFoundError: If the subjects path does not exist.
    """
    subjects_path = site_path / "raw"
    interviews: List[Interview] = []

    if not subjects_path.exists():
        raise FileNotFoundError(f"Subjects path {subjects_path} does not exist")

    for subject_path in subjects_path.iterdir():
        if not subject_path.is_dir():
            continue

        interviews += get_interviews_from_subject(subject_path)

    return interviews


def get_all_interviews(config_file: Path, data_root: Path) -> None:
    """
    Retrieves all interviews from the specified data root directory and imports them into a database.

    Args:
        config_file (Path): The path to the configuration file.
        data_root (Path): The root directory containing the interview data.

    Returns:
        None
    """
    sites_path = data_root / "PROTECTED"
    interviews: List[Interview] = []
    global INVALID_INTERVIEW_NAMES_COUNT

    for site_path in sites_path.iterdir():
        if not site_path.is_dir():
            continue

        site_name = site_path.name
        if site_name == "box_transfer":
            continue

        try:
            interviews += get_interviews_from_site(site_path)
        except FileNotFoundError as e:
            logger.warn(f"Site {site_name} has no raw data")

    logger.info(f"Got {len(interviews)} interviews")
    logger.warning(f"Invalid interviews count: {INVALID_INTERVIEW_NAMES_COUNT}")

    sql_queries: List[str] = []
    for interview in interviews:
        query = interview.to_sql()
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

    logger.info("Getting all interviews")
    get_all_interviews(config_file=config_file, data_root=data_root)

    logger.info("Done")
