#!/usr/bin/env python

import sys
from pathlib import Path
from typing import List

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


class Site:
    def __init__(self, site_id: str, site_name: str, country: str, network: str):
        self.site_id = site_id
        self.site_name = db.santize_string(site_name)
        self.country = country
        self.network = network

    def __str__(self) -> str:
        return f"Site({self.site_id}, {self.site_name}, {self.country}, {self.network})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE sites (
            site_id TEXT PRIMARY KEY,
            site_name TEXT NOT NULL,
            country TEXT NOT NULL,
            network TEXT NOT NULL
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS sites;
        """

        return sql_query

    def to_sql(self) -> str:
        sql_query = f"""
        INSERT INTO sites (site_id, site_name, country, network)
        VALUES ('{self.site_id}', '{self.site_name}', '{self.country}', '{self.network}');
        """

        return sql_query
