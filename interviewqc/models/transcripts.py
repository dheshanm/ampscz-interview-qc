#!/usr/bin/env python
from pathlib import Path
from typing import List

from interviewqc.helpers import db
from interviewqc.models.file import File


class Transcript:
    def __init__(self, transcript_path: Path, interview_name: str):
        self.transcript_path = transcript_path
        self.interview_name = interview_name

    def __str__(self):
        return f"Transcript({self.transcript_path}, {self.interview_name})"

    def __repr__(self):
        """
        Return a string representation of the Transcript object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'transcripts' table.
        """
        sql_query = """
        CREATE TABLE transcripts (
            transcript_path TEXT NOT NULL REFERENCES files (file_path),
            interview_name TEXT NOT NULL REFERENCES interviews (interview_name),
            PRIMARY KEY (transcript_path, interview_name)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'transcripts' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS transcripts;
        """

        return sql_query

    def to_sql(self) -> List[str]:
        """
        Return the SQL query to insert the File object into the 'transcripts' table.
        """
        sql_queries: List[str] = []
        t_path = db.santize_string(str(self.transcript_path))

        file = File.from_path(self.transcript_path)
        sql_queries.append(file.to_sql())

        sql_query = f"""
        INSERT INTO transcripts (transcript_path, interview_name)
        VALUES ('{t_path}', '{self.interview_name}');
        """
        sql_queries.append(sql_query)

        return sql_queries
