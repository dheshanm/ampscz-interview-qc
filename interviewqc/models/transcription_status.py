#!/usr/bin/env python
"""
A Model to represent the status of a transcription.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "ampscz-interview-qc":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from typing import Optional

from interviewqc.helpers import db, utils


class TranscriptionStatus:
    """
    A class to represent the status of a transcription
    """
    def __init__(self,
            subject_id: str,
            study_id: str,
            interview_type: str,
            interview_name: str,
            session: Optional[int],
            pipeline_status: str,
            transcript_file_status: str,
            qc_status: str,
            interview_length_minutes: Optional[float]
        ) -> None:
        self.subject_id = subject_id
        self.study_id = study_id
        self.interview_type = interview_type
        self.interview_name = interview_name
        self.session = session
        self.pipeline_status = pipeline_status
        self.transcript_file_status = transcript_file_status
        self.qc_status = qc_status
        self.interview_length_minutes = interview_length_minutes


    def __str__(self):
        return f"TranscriptionStatus({self.subject_id}, {self.study_id},\
{self.interview_type}, {self.interview_name}, {self.session}, {self.pipeline_status}, \
{self.transcript_file_status}, {self.qc_status}, {self.interview_length_minutes})"

    def __repr__(self):
        """
        Return a string representation of the TranscriptStatus object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'transcripts' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS transcription_status (
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            interview_type TEXT NOT NULL,
            interview_name TEXT NOT NULL,
            session INTEGER,
            pipeline_status TEXT NOT NULL,
            transcript_file_status TEXT NOT NULL,
            qc_status TEXT NOT NULL,
            interview_length_minutes REAL,
            PRIMARY KEY (interview_name),
            UNIQUE (subject_id, study_id, interview_type, session)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'transcription_status' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS transcription_status;
        """

        return sql_query

    def to_sql(self) ->str:
        """
        Return the SQL query to insert the TranscriptionStatus object into the
        'transcription_status' table.
        """
        if self.session is None:
            session = "NULL"
        else:
            session = self.session

        if self.interview_length_minutes is None:
            interview_length_minutes = "NULL"
        else:
            interview_length_minutes = self.interview_length_minutes

        sql_query = f"""
        INSERT INTO transcription_status (
            subject_id, study_id, interview_type, interview_name,
            session, pipeline_status, transcript_file_status,
            qc_status, interview_length_minutes
        ) VALUES (
            '{self.subject_id}', '{self.study_id}', '{self.interview_type}','{self.interview_name}',
            {session}, '{self.pipeline_status}', '{self.transcript_file_status}',
            '{self.qc_status}', {interview_length_minutes}
        ) ON CONFLICT (subject_id, study_id, interview_type, session) DO NOTHING;
        """

        sql_query = db.handle_nan(sql_query)

        return sql_query

if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console = utils.get_console()

    console.log("Initializing 'transcription_status' table...")
    console.log(
        "[red]This will delete all existing data in the 'transcription_status' table!"
    )

    drop_queries = [TranscriptionStatus.drop_table_query()]
    init_queries = [TranscriptionStatus.init_table_query()]

    sql_queries = drop_queries + init_queries

    console.log("Executing queries...")
    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("[green]Done!")
