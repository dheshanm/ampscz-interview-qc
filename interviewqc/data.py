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
