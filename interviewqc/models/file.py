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

from interviewqc.helpers.hash import compute_hash


class File:
    def __init__(
        self,
        file_name: str,
        file_type: str,
        file_size: str,
        file_path: Path,
        m_time: datetime,
    ):
        self.file_name = file_name
        self.file_type = file_type
        self.file_size = file_size
        self.file_path = file_path
        self.m_time = m_time
        self.md5 = compute_hash(file_path=file_path, hash_type="md5")

    def __str__(self):
        return f"File({self.file_name}, {self.file_type}, {self.file_size}, \
            {self.file_path}, {self.m_time}, {self.md5})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE file (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size INT NOT NULL,
            file_path TEXT PRIMARY KEY,
            m_time TIMESTAMP NOT NULL,
            md5 TEXT NOT NULL
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS file;
        """

        return sql_query

    def to_sql(self):
        sql_query = f"""
        INSERT INTO file (file_name, file_type, file_size,
            file_path, m_time, md5)
        VALUES ('{self.file_name}', '{self.file_type}', '{self.file_size}',
            '{self.file_path}', '{self.m_time}', '{self.md5}');
        """

        return sql_query
