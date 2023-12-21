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

from interviewqc.helpers import db


class InterviewRaw:
    """
    Maps a file to an interview.

    Attributes:
        interview_path (Path): The path to the interview.
        file_path (Path): The path to the file associated with the interview.
    """

    def __init__(self, interview_path: Path, file_path: Path):
        """
        Initializes a new instance of the InterviewRaw class.

        Args:
            interview_path (Path): The path to the interview.
            file_path (Path): The path to the file associated with the interview.
        """
        self.interview_path = interview_path
        self.file_path = file_path

    def __repr__(self):
        """
        Returns a string representation of the InterviewRaw object.

        Returns:
            str: A string representation of the InterviewRaw object.
        """
        return f"<InterviewRaw {self.interview_path} {self.file_path}>"

    def __str__(self):
        """
        Returns a string representation of the InterviewRaw object.

        Returns:
            str: A string representation of the InterviewRaw object.
        """
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query for creating the interview_raw table.

        Returns:
            str: The SQL query for creating the interview_raw table.
        """
        sql_query = """
        CREATE TABLE interview_raw (
            interview_path TEXT NOT NULL,
            file_path TEXT NOT NULL PRIMARY KEY,
            FOREIGN KEY (interview_path) REFERENCES interviews (interview_path),
            FOREIGN KEY (file_path) REFERENCES files (file_path)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query for dropping the interview_raw table.

        Returns:
            str: The SQL query for dropping the interview_raw table.
        """
        sql_query = """
        DROP TABLE IF EXISTS interview_raw;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL query for inserting the InterviewRaw object into the interview_raw table.

        Returns:
            str: The SQL query for inserting the InterviewRaw object into the interview_raw table.
        """
        i_path = db.santize_string(str(self.interview_path))
        f_path = db.santize_string(str(self.file_path))

        sql_query = f"""
        INSERT INTO interview_raw (interview_path, file_path)
        VALUES ('{i_path}', '{f_path}')
        """

        return sql_query
