import psycopg2
import os
from dotenv import load_dotenv
from sql_queries import create_tables_statement, drop_tables_statement

load_dotenv()

DWH_DB_USER = os.getenv("DWH_DB_USER")
DWH_DB_PASSWORD = os.getenv("DWH_DB_PASSWORD")
DWH_DB_PORT = os.getenv("DWH_DB_PORT")
DWH_DB = os.getenv("DWH_DB")
DWH_ENDPOINT = os.getenv("DWH_ENDPOINT")


# drop tables
def drop_tables(conn, cur, sql):
    """
    drop fact and dimensional table in the data warehouse
    :param conn: database connection
    :param cur: database cursor
    :param sql: list of sql statements
    :return:
    """
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print(f"drop table: {query.split(' ')[-1]}")
        except psycopg2.Error as e:
            print("Error dropping tables")
            print(e)


# create tables
def create_tables(conn, cur, sql):
    """
    Create fact and dimensional table in the data warehouse
    :param conn: database connection
    :param cur: database cursor
    :param sql: list of sql statements
    :return:
    """
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print(f"table: {query.split(' ')[10]} created")
        except psycopg2.Error as e:
            print("Error creating tables")
            print(e)


def establish_connection():
    """
    This function create a connection to redshift data warehouse using psycopg2
    :return: conn, cur
    """
    try:
        conn = psycopg2.connect(host=DWH_ENDPOINT, database=DWH_DB, user=DWH_DB_USER, password=DWH_DB_PASSWORD,
                                port=DWH_DB_PORT)
        cur = conn.cursor()
        print("established connection to database")
    except psycopg2.Error as e:
        print("error establishing connection")
        print(e)

    return conn, cur


def main():
    # establish connection
    conn, cur = establish_connection()

    # drop tables if already created
    drop_tables(conn=conn, cur=cur, sql=drop_tables_statement)

    # create tables in our redshift data warehouse
    create_tables(conn=conn, cur=cur, sql=create_tables_statement)


if __name__ == "__main__":
    main()
