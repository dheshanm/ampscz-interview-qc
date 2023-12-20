class InterviewRaw:
    def __init__(self, interview_id: int, file_path: str):
        self.interview_id = interview_id
        self.file_path = file_path

    def __repr__(self):
        return f"<InterviewRaw {self.interview_id} {self.file_path}>"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE interview_raw (
            interview_id INTEGER,
            file_path TEXT,
            FOREIGN KEY (interview_id) REFERENCES interviews (interview_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS interview_raw;
        """

        return sql_query

    def to_sql(self) -> str:
        sql_query = f"""
        INSERT INTO interview_raw (interview_id, file_path)
        VALUES ({self.interview_id}, '{self.file_path}');
        """

        return sql_query
