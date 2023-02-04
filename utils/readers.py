import logging
import sqlite3

import pandas as pd


def get_cursor_from_db_file(db: str):
    """
    Establishes connection to the SQLite database
    based on the path to the .db file and returns
    cursor of that connection

    :param db: path to db file
    :return: sqlite3.Cursor object
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    return cursor


def execute_query(cursor: sqlite3.Cursor, query: str):
    """
    Executes the query given as argument on the SQLite
    cursor object

    :param cursor: Cursor object
    :param query: query which needs to be executed on
    the cursor given as an argument

    :return: List of tuples, representing the result
    (rows of a table) of the query execution
    """
    res_obj = cursor.execute(query)  # we can also use .fetchall()

    rows = [x for x in res_obj]

    return rows


if __name__ == '__main__':
    _conn = get_cursor_from_db_file('<path/to/db/file>')
    # data = execute_query(conn, """Select * from camera""")
    # data = execute_query(conn, """SELECT name FROM sqlite_master WHERE type='table';""")
    _data = execute_query(_conn, """PRAGMA table_info('camera')""")

    _conn = sqlite3.connect('../cctv_recordings/2022081205/backup.db3')

    data = pd.read_sql_query("SELECT * from camera", _conn)

    logging.info(data)
