"""
This is the module, representing reader util class for ImiLab CCTV Manager
"""

import logging
import sqlite3
from typing import List, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO)

MAIN_TABLE_NAME = 'camera'


class ImiLabCCTVVideoManagerReader:
    """
    This class provides reading capabilities for ImiLab CCTV Manager
    """

    def __init__(self, db_file_path):
        """
        Initializes a new instance of the ImiLabCCTVVideoManagerReader class by
        attempting to connect to the db file

        :param db_file_path: Pathlike string referring the path to the db file
        """
        try:
            self.__conn = sqlite3.connect(db_file_path)
        except sqlite3.OperationalError as not_found:
            logging.info('DB file not found: %s', not_found)

            self.__conn = None

    def is_connected(self) -> bool:
        """
        Checks whether ImiLab CCTV Manager Reader is connected to DB or not

        :return: boolean indicating whether ImiLab CCTV is connected to DB or not
        """
        return bool(self.__conn)

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
        rows = list(res_obj)

        return rows

    def retrieve_table(self, table_name: str) -> pd.DataFrame:
        """
        Retrieves the table from the SQLite db

        :param table_name: name of the table to be retrieved
        :return: Table content as a Dataframe
        """
        df = pd.read_sql_query(f"SELECT * from {table_name}", con=self.__conn)

        return df
