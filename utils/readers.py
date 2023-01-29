import sqlite3


def get_conn_from_db_file(db: str):
    """
    Establishes connection to the SQLite database
    based on the path to the .db file

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
    res_obj = cursor.execute(query)

    rows = [x for x in res_obj]

    return rows
