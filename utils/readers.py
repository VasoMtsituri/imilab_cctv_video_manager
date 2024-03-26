import logging
import sqlite3
from typing import List, Tuple

logging.basicConfig(level=logging.INFO)

MAIN_TABLE_NAME = 'camera'


class ImiLabCCTVVideoManager:
    def __init__(self, db_file_path):
        try:
            self.__conn = sqlite3.connect(db_file_path)
        except sqlite3.OperationalError as not_found:
            logging.info(f'DB file not found: {not_found}')

            self.__conn = None

    def is_connected(self) -> bool:
        return True if self.__conn else False

    def get_cursor(self) -> sqlite3.Cursor:
        """
        Returns the cursor of the connection

        :return: sqlite3.Cursor object
        """

        return self.__conn.cursor()

    def execute_query(self, query: str) -> List[Tuple]:
        """
        Executes the query given as argument on the SQLite cursor object

        :param query: query which needs to be executed on the cursor given as an argument
        :return: List of tuples, representing the result (rows of a table) of the query execution
        """
        cursor = self.get_cursor()
        res_obj = cursor.execute(query)  # we can also use .fetchall()
        rows = [x for x in res_obj]

        return rows

    # TODO: Needs to be reviewed, because the following warning:
    # "pandas only supports SQLAlchemy connectable (engine/connection)
    #  or database string URI or sqlite3 DBAPI2 connection"

    # def retrieve_table(self, table_name: str) -> pd.DataFrame:
    #     """
    #     Retrieves the table from the SQLite db
    #
    #     :param table_name: name of the table to be retrieved
    #     :return: Table content as a Dataframe
    #     """
    #     df = pd.read_sql_table(f"SELECT * from {table_name}", con=self.get_cursor())
    #
    #     return df


if __name__ == '__main__':
    cctv_manager = ImiLabCCTVVideoManager(db_file_path='db_file_path')

    data = cctv_manager.execute_query(f"SELECT * from {MAIN_TABLE_NAME}")

    logging.info(data)
