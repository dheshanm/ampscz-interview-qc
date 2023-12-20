class Site:
    def __init__(self, site_id: str, site_name: str, country: str, network: str):
        self.site_id = site_id
        self.site_name = site_name
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
