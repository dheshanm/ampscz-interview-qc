from datetime import datetime


class Subject:
    def __init__(self, subject_id: str, site_id: str, consent_date: datetime) -> None:
        self.subject_id = subject_id
        self.site_id = site_id
        self.consent_date = consent_date

    def __str__(self) -> str:
        return f"Subject({self.subject_id})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE subjects (
            subject_id TEXT PRIMARY KEY,
            site_id TEXT NOT NULL,
            consent_date TIMESTAMP NOT NULL,
            FOREIGN KEY (site_id) REFERENCES sites (site_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS subjects;
        """

        return sql_query

    def to_sql(self) -> str:
        sql_query = f"""
        INSERT INTO subjects (subject_id, site_id, consent_date)
        VALUES ('{self.subject_id}', '{self.site_id}', '{self.consent_date.strftime("%Y-%m-%d %H:%M:%S")}')
        ON CONFLICT (subject_id) DO NOTHING;
        """

        return sql_query
