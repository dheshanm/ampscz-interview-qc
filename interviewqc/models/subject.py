class Subject:
    def __init__(self, subject_id: str, site_id: str):
        self.subject_id = subject_id
        self.site_id = site_id

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
        INSERT INTO subjects (subject_id, site_id)
        VALUES ('{self.subject_id}', '{self.site_id}');
        """

        return sql_query
