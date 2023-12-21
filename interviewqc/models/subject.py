from datetime import datetime


class Subject:
    """
    Represents a subject.

    Attributes:
        subject_id (str): The ID of the subject.
        site_id (str): The ID of the site.
        consent_date (datetime): The date of consent.
    """

    def __init__(self, subject_id: str, site_id: str, consent_date: datetime) -> None:
        """
        Initialize a Subject object.

        Args:
            subject_id (str): The ID of the subject.
            site_id (str): The ID of the site.
            consent_date (datetime): The date of consent.

        Returns:
            None
        """
        self.subject_id = subject_id
        self.site_id = site_id
        self.consent_date = consent_date

    def __str__(self) -> str:
        """
        Return a string representation of the Subject object.

        Returns:
            str: The string representation of the Subject object.
        """
        return f"Subject({self.subject_id})"

    def __repr__(self) -> str:
        """
        Return a string representation of the Subject object.

        Returns:
            str: The string representation of the Subject object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Get the SQL query to create the subjects table.

        Returns:
            str: The SQL query to create the subjects table.
        """
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
        """
        Get the SQL query to drop the subjects table.

        Returns:
            str: The SQL query to drop the subjects table.
        """
        sql_query = """
        DROP TABLE IF EXISTS subjects;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Get the SQL query to insert the Subject object into the subjects table.

        Returns:
            str: The SQL query to insert the Subject object into the subjects table.
        """
        sql_query = f"""
        INSERT INTO subjects (subject_id, site_id, consent_date)
        VALUES ('{self.subject_id}', '{self.site_id}', '{self.consent_date.strftime("%Y-%m-%d %H:%M:%S")}')
        ON CONFLICT (subject_id) DO NOTHING;
        """

        return sql_query
