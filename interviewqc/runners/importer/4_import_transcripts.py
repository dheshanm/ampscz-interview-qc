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
from typing import List, Optional

from rich.logging import RichHandler

from interviewqc.helpers import utils, db
from interviewqc.helpers.config import config
from interviewqc.models.transcripts import Transcript


MODULE_NAME = "interviewqc_import_transcripts"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_interview_name(
    config_file: Path, subject_id: str, interview_type: str, days_since_consent: int
) -> Optional[str]:
    """
    Retrieves the name of the interview from the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The ID of the subject.
        interview_type (str): The type of interview.
        days_since_consent (int): The day of the interview.

    Returns:
        Optional[str]: The name of the interview.

    Raises:
        ValueError: If there are multiple interviews for the specified subject and day.
    """
    query = f"""
        SELECT
            interview_name
        FROM
            interviews
        WHERE
            subject_id = '{subject_id}'
            AND interview_type = '{interview_type}'
            AND days_since_consent = {days_since_consent};
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    if df.shape[0] > 1:
        raise ValueError(
            f"Got multiple interviews for subject {subject_id} on day {days_since_consent}"
        )

    interview_name = df.iloc[0]["interview_name"]

    return interview_name


def get_transcript_days_since_consent(transcript_path: Path) -> int:
    """
    Parses the specified transcript file path and returns the day of the interview.

    e.g. "PrescientGW_GW92127_interviewAudioTranscript_open_day0016_session001.txt" -> 16

    Args:
        transcript_path (Path): The path to the transcript file.

    Returns:
        int: The day of the interview.
    """
    transcript_name = transcript_path.name
    transcript_name = transcript_name.replace(".txt", "")

    transcript_name_parts = transcript_name.split("_")
    day = transcript_name_parts[-2]
    day = day.replace("day", "")
    day_int = int(day)

    return day_int


def get_transcripts_from_dir(
    config_file: Path, interview_type_path: Path, subject_id: str, interview_type: str
) -> List[Transcript]:
    """
    Retrieves a list of transcripts from the specified interview type path.

    Args:
        interview_type_path (Path): The path to the interview type directory.
        subject_id (str): The ID of the subject.
        interview_type (str): The type of interview.

    Returns:
        List[Transcript]: A list of Transcript objects.
    """
    transcripts: List[Transcript] = []

    transcripts_path = interview_type_path.glob("*.txt")

    for transcript_path in transcripts_path:
        days_since_consent = get_transcript_days_since_consent(transcript_path)
        try:
            interview_name = get_interview_name(
                config_file=config_file,
                subject_id=subject_id,
                interview_type=interview_type,
                days_since_consent=days_since_consent,
            )
        except ValueError:
            logger.warning(
                f"Got multiple interviews for subject {subject_id} on day {days_since_consent}."
            )
            logger.warning(f"Skipping transcript {transcript_path}")
            continue

        if interview_name is None:
            logger.warning(
                f"Could not find interview for subject {subject_id} on day {days_since_consent}"
            )
            continue

        transcript = Transcript(
            transcript_path=transcript_path, interview_name=interview_name
        )

        transcripts.append(transcript)

    return transcripts


def get_transcripts_from_subject(
    config_file: Path,
    subject_path: Path,
) -> List[Transcript]:
    """
    Retrieves a list of interviews from the specified subject path.

    Args:
        subject_path (Path): The path to the subject directory.

    Returns:
        List[Transcript]: A list of Interview objects.
    """
    subject_id = subject_path.name
    interview_path = subject_path / "interviews"

    transcripts: List[Transcript] = []

    interview_types = ["open", "psychs"]

    for interview_type in interview_types:
        interview_type_path = interview_path / interview_type
        if not interview_type_path.exists():
            continue

        subject_transcripts = get_transcripts_from_dir(
            config_file=config_file,
            interview_type=interview_type,
            interview_type_path=interview_type_path,
            subject_id=subject_id,
        )
        transcripts.extend(subject_transcripts)

    return transcripts


def get_transcripts_from_site(
    config_file: Path,
    site_path: Path,
) -> List[Transcript]:
    """
    Retrieves a list of transcripts from the specified site path.

    Args:
        site_path (Path): The path to the site directory.

    Returns:
        List[Transcript]: A list of Transcript objects.

    Raises:
        FileNotFoundError: If the subjects path does not exist.
    """
    subjects_path = site_path / "processed"
    transcripts: List[Transcript] = []

    if not subjects_path.exists():
        raise FileNotFoundError(f"Subjects path {subjects_path} does not exist")

    for subject_path in subjects_path.iterdir():
        if not subject_path.is_dir():
            continue

        subject_transcripts = get_transcripts_from_subject(
            config_file=config_file, subject_path=subject_path
        )
        transcripts.extend(subject_transcripts)

    return transcripts


def import_all_transcripts(config_file: Path, data_root: Path) -> None:
    """
    Retrieves all transcripts from the specified data root directory and imports them into a database.

    Args:
        config_file (Path): The path to the configuration file.
        data_root (Path): The root directory containing the study data.

    Returns:
        None
    """
    sites_path = data_root / "PROTECTED"
    transcripts: List[Transcript] = []

    for site_path in sites_path.iterdir():
        if not site_path.is_dir():
            continue

        site_name = site_path.name
        if site_name == "box_transfer":
            continue

        try:
            site_transcripts = get_transcripts_from_site(
                config_file=config_file, site_path=site_path
            )
            transcripts.extend(site_transcripts)
            logger.info(f"Got {len(site_transcripts)} transcripts from site {site_name}")

        except FileNotFoundError:
            logger.warning(f"Site {site_name} has no raw data")

    logger.info(f"Got {len(transcripts)} transcripts")

    sql_queries: List[str] = []

    for transcript in transcripts:
        queries = transcript.to_sql()
        sql_queries.extend(queries)

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
    import_all_transcripts(config_file=config_file, data_root=data_root)

    logger.info("Done")
