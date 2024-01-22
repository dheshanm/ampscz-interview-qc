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

from interviewqc.helpers import db
from interviewqc.helpers.hash import compute_hash


class MovedFile:
    """
    Represents a file.

    Attributes:
        file_name (str): The name of the file.
        file_type (str): The type of the file.
        file_size (float): The size of the file in bytes.
        file_path (Path): The path to the file.
        m_time (datetime): The modification time of the file.
        md5 (str): The MD5 hash of the file.
    """

    def __init__(self, source_file_path: Path, destination_file_path: Path):
        """
        Initialize a MovedFile object.

        Args:
            source_file_path (Path): The path to the source file.
            destination_file_path (Path): The path to the destination file.
        """
        self.source_file_path = source_file_path
        self.destination_file_path = destination_file_path
        self.md5 = compute_hash(self.destination_file_path)
        self.timestamp = datetime.now()

    def __str__(self):
        """
        Return a string representation of the MovedFile object.
        """
        return f"MovedFile({self.source_file_path}, {self.destination_file_path})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE TABLE moved_files (
            source_file_path TEXT NOT NULL,
            destination_file_path TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            md5 TEXT NOT NULL,
            PRIMARY KEY (source_file_path, destination_file_path)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'moved_files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS moved_files;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the File object into the 'moved_files' table.
        """
        sf_path = db.santize_string(str(self.source_file_path))
        df_path = db.santize_string(str(self.destination_file_path))

        sql_query = f"""
        INSERT INTO moved_files (source_file_path, destination_file_path, timestamp, md5)
        VALUES ('{sf_path}', '{df_path}', '{self.timestamp}', '{self.md5}');
        """

        return sql_query
