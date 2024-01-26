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

from datetime import datetime
from typing import Optional

from interviewqc.helpers import db


class Interview:
    """
    Represents an interview.

    Args:
        interview_path (Path): The path to the interview.
        interview_name (str): The name of the interview.
        interview_type (str): The type of the interview (open, psychs, etc.)
        subject_id (str): The ID of the subject.
        interview_date (Optional[datetime], optional): The date of the interview. Defaults to None.
        has_additional_files (bool, optional): Indicates if the interview has additional files. Defaults to False.
    """

    def __init__(
        self,
        interview_path: Path,
        interview_name: str,
        interview_type: str,
        subject_id: str,
        interview_date: Optional[datetime] = None,
        days_since_consent: Optional[int] = None,
        has_additional_files: bool = False,
    ):
        if not interview_path.exists():
            raise FileNotFoundError(f"Interview path {interview_path} does not exist")

        self.interview_path = interview_path
        self.days_since_consent = days_since_consent
        self.interview_name = interview_name
        self.interview_type = interview_type
        self.subject_id = subject_id
        self.interview_date = interview_date
        self.has_additional_files = has_additional_files

    def __str__(self) -> str:
        return f"Interview({self.interview_name}, {self.interview_type}, {self.subject_id})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE interviews (
            subject_id TEXT NOT NULL REFERENCES subjects (subject_id),
            days_since_consent INTEGER,
            interview_path TEXT PRIMARY KEY,
            interview_name TEXT NOT NULL UNIQUE,
            interview_type TEXT NOT NULL,
            interview_date TIMESTAMP,
            has_additional_files BOOLEAN DEFAULT FALSE
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS interviews;
        """

        return sql_query

    def to_sql(self) -> str:
        i_path = db.santize_string(str(self.interview_path))
        i_name = db.santize_string(self.interview_name)

        if self.interview_date is None:
            i_date = "NULL"
        else:
            i_date = self.interview_date.strftime("%Y-%m-%d %H:%M:%S")

        if self.days_since_consent is None:
            self.days_since_consent = "NULL"

        if self.has_additional_files is None:
            self.has_additional_files = False

        sql_query = f"""
        INSERT INTO interviews (interview_path, interview_name, interview_type, \
            interview_date, subject_id, days_since_consent, has_additional_files)
        VALUES ('{i_path}', '{i_name}', '{self.interview_type}', \
            '{i_date}', '{self.subject_id}', {self.days_since_consent}, {self.has_additional_files});
        """

        sql_query = db.handle_null(sql_query)

        return sql_query
