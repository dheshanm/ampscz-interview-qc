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
from typing import List, Tuple, Set
from datetime import datetime

from rich.logging import RichHandler

from interviewqc.helpers import utils, db, dpdash
from interviewqc.helpers.config import config
from interviewqc.models.interview import Interview
from interviewqc.models.oosop_interviews import OutOfSopInterview
from interviewqc import data


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


def compute_days_since_consent(
    config_file: Path, interview_date: datetime, subject_id: str
) -> int:
    """
    Computes the number of days since the subject consented to the study.

    Args:
        config_file (Path): The path to the configuration file.
        interview_date (datetime): The date of the interview.
        subject_id (str): The ID of the subject.

    Returns:
        int: The number of days since the subject consented to the study.
    """
    consent_date = data.get_consent_data(config_file=config_file, subject_id=subject_id)

    if consent_date is None:
        raise ValueError(f"Subject {subject_id} has no consent date")

    days_since_consent = (interview_date - consent_date).days + 1
    return days_since_consent


def get_interviews_from_file(
    interviews_file: Path, subject_id: str, interview_type: str
) -> Tuple[List[Interview], List[OutOfSopInterview]]:
    """
    Retrieves a list of Interview objects from the specified file.

    Args:
        interviews_file (Path): The file containing the interviews.
        subject_id (str): The ID of the subject.
        interview_type (str): The type of the interview.

    Returns:
        List[Interview]: A list of Interview objects.

    """
    interviews: List[Interview] = []
    out_of_sop_interviews: List[OutOfSopInterview] = []
    global INVALID_INTERVIEW_NAMES_COUNT

    file_name = interviews_file.name
    if interviews_file.suffix != ".wav" and interviews_file.suffix != ".WAV":
        logger.warning(
            f"Interview '{file_name}' has invalid extension: {interviews_file.suffix}"
        )
        return interviews, out_of_sop_interviews
    if file_name.startswith("."):
        return interviews, out_of_sop_interviews

    if len(interviews_file.stem) != 14:
        valid_name = False
        INVALID_INTERVIEW_NAMES_COUNT += 1
        logger.warning(f"Interview '{interviews_file}' has Out-of-SOP name")
    else:
        valid_name = True

    interview_datetime_str = file_name[:14]
    interview_datetime = datetime.strptime(interview_datetime_str, "%Y%m%d%H%M%S")

    days_since_consent = compute_days_since_consent(
        config_file=config_file,
        interview_date=interview_datetime,
        subject_id=subject_id,
    )

    consent_date = data.get_consent_data(config_file=config_file, subject_id=subject_id)

    timepoint = dpdash.get_dpdash_timepoint(
        consent_date=consent_date, event_date=interview_datetime
    )

    interview_name = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="interview",
        category=interview_type,
        optional_tag=None,
        time_range=timepoint,
    )

    if valid_name:
        interview = Interview(
            interview_path=interviews_file,
            days_since_consent=days_since_consent,
            interview_name=interview_name,
            interview_type=interview_type,
            subject_id=subject_id,
            interview_date=interview_datetime,
        )

        interviews.append(interview)
    else:
        oo_sop_interview = OutOfSopInterview(
            interview_path=interviews_file,
            days_since_consent=days_since_consent,
            interview_name=interview_name,
            interview_type=interview_type,
            subject_id=subject_id,
            interview_date=interview_datetime,
            note="Invalid Interview Name",
        )

        out_of_sop_interviews.append(oo_sop_interview)

    return interviews, out_of_sop_interviews


def get_interviews_from_dir(
    interviews_dir: Path, subject_id: str, interview_type: str
) -> Tuple[List[Interview], List[OutOfSopInterview]]:
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
    out_of_sop_interviews: List[OutOfSopInterview] = []
    global INVALID_INTERVIEW_NAMES_COUNT

    for interview_dir in interviews_dir.iterdir():
        if not interview_dir.is_dir():
            file_interviews, file_oosop_interviews = get_interviews_from_file(
                interviews_file=interview_dir,
                subject_id=subject_id,
                interview_type=interview_type,
            )

            interviews.extend(file_interviews)
            out_of_sop_interviews.extend(file_oosop_interviews)
            continue

        interview_file_name = interview_dir.name
        valid_name = True
        try:
            interview_datetime = interview_file_name[:19]
            interview_date = datetime.strptime(interview_datetime, "%Y-%m-%d %H.%M.%S")
        except ValueError:
            if (
                not interview_file_name == "Audio Record"
                and not interview_file_name == "for review"
            ):
                logger.warning(f"Interview '{interview_dir}' has Out-of-SOP name")
                interview_date = None
                valid_name = False
                INVALID_INTERVIEW_NAMES_COUNT += 1
            else:
                continue

        if interview_date is None:
            days_sice_consent = None
        else:
            days_sice_consent = compute_days_since_consent(
                config_file=config_file,
                interview_date=interview_date,
                subject_id=subject_id,
            )

        consent_date = data.get_consent_data(
            config_file=config_file, subject_id=subject_id
        )

        if interview_date is not None:
            timepoint = dpdash.get_dpdash_timepoint(
                consent_date=consent_date, event_date=interview_date
            )
        else:
            timepoint = "day0"

        interview_name = dpdash.get_dpdash_name(
            study=subject_id[:2],
            subject=subject_id,
            data_type="interview",
            category=interview_type,
            optional_tag=None,
            time_range=timepoint,
        )

        if valid_name:
            interview = Interview(
                interview_path=interview_dir,
                days_since_consent=days_sice_consent,
                interview_name=interview_name,
                interview_type=interview_type,
                subject_id=subject_id,
                interview_date=interview_date,
            )

            interviews.append(interview)
        else:
            out_of_sop_interview = OutOfSopInterview(
                interview_path=interview_dir,
                days_since_consent=days_sice_consent,
                interview_name=interview_name,
                interview_type=interview_type,
                subject_id=subject_id,
                interview_date=interview_date,
                note="Invalid Interview Name",
            )

            out_of_sop_interviews.append(out_of_sop_interview)

    return interviews, out_of_sop_interviews


def get_interviews_from_subject(
    subject_path: Path,
) -> Tuple[List[Interview], List[OutOfSopInterview]]:
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
    out_of_sop_interviews: List[OutOfSopInterview] = []

    # open Interviews
    open_interviews_path = interview_path / "open"

    if open_interviews_path.exists():
        subject_interviews, subject_oosop_interviews = get_interviews_from_dir(
            open_interviews_path, subject_id, "open"
        )
        interviews.extend(subject_interviews)
        out_of_sop_interviews.extend(subject_oosop_interviews)

    # psychs Interviews
    psychs_interviews_path = interview_path / "psychs"

    if psychs_interviews_path.exists():
        subject_interviews, subject_oosop_interviews = get_interviews_from_dir(
            psychs_interviews_path, subject_id, "psychs"
        )
        interviews.extend(subject_interviews)
        out_of_sop_interviews.extend(subject_oosop_interviews)

    return interviews, out_of_sop_interviews


def get_interviews_from_site(
    site_path: Path,
) -> Tuple[List[Interview], List[OutOfSopInterview]]:
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
    out_of_sop_interviews: List[OutOfSopInterview] = []

    if not subjects_path.exists():
        raise FileNotFoundError(f"Subjects path {subjects_path} does not exist")

    for subject_path in subjects_path.iterdir():
        if not subject_path.is_dir():
            continue

        subject_interviews, subject_out_of_sop_interviews = get_interviews_from_subject(
            subject_path
        )
        interviews.extend(subject_interviews)
        out_of_sop_interviews.extend(subject_out_of_sop_interviews)

    return interviews, out_of_sop_interviews


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
    out_of_sop_interviews: List[OutOfSopInterview] = []
    global INVALID_INTERVIEW_NAMES_COUNT

    for site_path in sites_path.iterdir():
        if not site_path.is_dir():
            continue

        site_name = site_path.name
        if site_name == "box_transfer":
            continue

        try:
            site_interviews, site_out_of_sop_interviews = get_interviews_from_site(
                site_path
            )
            interviews.extend(site_interviews)
            out_of_sop_interviews.extend(site_out_of_sop_interviews)
        except FileNotFoundError:
            logger.warning(f"Site {site_name} has no raw data")

    logger.info(f"Got {len(interviews)} interviews")
    logger.warning(f"Invalid interviews count: {INVALID_INTERVIEW_NAMES_COUNT}")

    duplicate_interview_names: Set[str] = set()
    interviews_names: Set[str] = set()

    for interview in interviews:
        if interview.interview_name in interviews_names:
            duplicate_interview_names.add(interview.interview_name)

    sql_queries: List[str] = []
    duplicate_interviews: List[Interview] = []

    for interview in interviews:
        if interview.interview_name in duplicate_interview_names:
            duplicate_interviews.append(interview)
            continue
        query = interview.to_sql()
        sql_queries.append(query)

    for interview in out_of_sop_interviews:
        query = interview.to_sql()
        sql_queries.append(query)

    duplicate_oosop_interviews: List[OutOfSopInterview] = []
    for duplicate_interview in duplicate_interviews:
        oosop_interview = OutOfSopInterview(
            interview_path=duplicate_interview.interview_path,
            days_since_consent=duplicate_interview.days_since_consent,
            interview_name=duplicate_interview.interview_name,
            interview_type=duplicate_interview.interview_type,
            subject_id=duplicate_interview.subject_id,
            interview_date=duplicate_interview.interview_date,
            note="Duplicate Interview Name",
        )

        duplicate_oosop_interviews.append(oosop_interview)
        sql_queries.append(oosop_interview.to_sql())

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
