from pathlib import Path
from datetime import datetime
from typing import Optional

from interviewqc.helpers import db


def get_consent_data(config_file: Path, subject_id: str) -> Optional[datetime]:
    query = f"""
        SELECT
            consent_date
        FROM
            subjects
        WHERE
            subject_id = '{subject_id}';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        return None

    date_dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    return date_dt


def compute_days_since_consent(
    config_file: Path, event_date: datetime, subject_id: str
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
    consent_date = get_consent_data(config_file=config_file, subject_id=subject_id)

    if consent_date is None:
        raise ValueError(f"Subject {subject_id} has no consent date")

    days_since_consent = (event_date - consent_date).days + 1
    return days_since_consent
