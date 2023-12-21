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


class File:
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

    def __init__(
        self,
        file_name: str,
        file_type: str,
        file_size: float,
        file_path: Path,
        m_time: datetime,
    ):
        """
        Initialize a File object.

        Args:
            file_name (str): The name of the file.
            file_type (str): The type of the file.
            file_size (float): The size of the file in bytes.
            file_path (Path): The path to the file.
            m_time (datetime): The modification time of the file.
        """
        self.file_name = file_name
        self.file_type = file_type
        self.file_size = file_size
        self.file_path = file_path
        self.m_time = m_time
        self.md5 = compute_hash(file_path=file_path, hash_type="md5")

    def __str__(self):
        """
        Return a string representation of the File object.
        """
        return f"File({self.file_name}, {self.file_type}, {self.file_size}, \
            {self.file_path}, {self.m_time}, {self.md5})"

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
        CREATE TABLE files (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size FLOAT NOT NULL,
            file_path TEXT PRIMARY KEY,
            m_time TIMESTAMP NOT NULL,
            md5 TEXT NOT NULL
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS files;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the File object into the 'files' table.
        """
        f_name = db.santize_string(self.file_name)
        f_path = db.santize_string(str(self.file_path))

        sql_query = f"""
        INSERT INTO files (file_name, file_type, file_size,
            file_path, m_time, md5)
        VALUES ('{f_name}', '{self.file_type}', '{self.file_size}',
            '{f_path}', '{self.m_time}', '{self.md5}');
        """

        return sql_query
