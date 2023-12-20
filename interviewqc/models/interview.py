class Interview:
    def __init__(
        self,
        interview_name: str,
        participant_id: str,
    ):
        self.interview_name = interview_name
        self.participant_id = participant_id

    def __str__(self) -> str:
        return f"Interview({self.interview_name}, {self.participant_id})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        sql_query = """
        CREATE TABLE interviews (
            interview_id INTEGER PRIMARY KEY,
            interview_name TEXT,
            participant_id TEXT
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        sql_query = """
        DROP TABLE IF EXISTS interviews;
        """

        return sql_query

    def to_sql(self) -> str:
        sql_query = f"""
        INSERT INTO interviews (interview_name, participant_id)
        VALUES ('{self.interview_name}', '{self.participant_id}');
        """

        return sql_query
