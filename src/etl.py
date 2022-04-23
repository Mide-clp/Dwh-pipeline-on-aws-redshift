from dotenv import load_dotenv
import psycopg2
import os
from sql_queries import copy_table_statement, insert_table_statement, select_statement

load_dotenv()

DWH_DB_USER = os.getenv("DWH_DB_USER")
DWH_DB_PASSWORD = os.getenv("DWH_DB_PASSWORD")
DWH_DB_PORT = os.getenv("DWH_DB_PORT")
DWH_DB = os.getenv("DWH_DB")
DWH_ENDPOINT = os.getenv("DWH_ENDPOINT")


def establish_connection():
    """

    :return: conn, cur
    """
    try:
        conn = psycopg2.connect(host=DWH_ENDPOINT, database=DWH_DB, user=DWH_DB_USER, password=DWH_DB_PASSWORD,
                                port=DWH_DB_PORT)
        cur = conn.cursor()
        print("established connection")
    except psycopg2.Error as e:
        print("error establishing connection")
        print(e)

    return conn, cur


def copy_table(conn, cur, sql):
    """
    Copy data into staging tables
    :param conn: database connection
    :param cur: database cursor
    :param sql: list of sql statements
    :return:
    """
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Copy data into {query.split(' ')[5]} table")
        except psycopg2.Error as e:
            print("error copying tables")
            print(e)


def insert_table(conn, cur, sql):
    """
    Insert data into fact and dimensional table in the data warehouse
    :param conn: database connection
    :param cur: database cursor
    :param sql: list of sql statements
    :return:
    """
    for query in sql:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Inserted data into table: {query.split(' ')[6]} ")
        except psycopg2.Error as e:
            print("Error inserting into tables")
            print(e)


def quality_checks(cur, sql):
    """

    :param cur:
    :param sql:
    :return:
    """
    for query in sql:
        try:
            cur.execute(query)
            result = cur.fetchone()
            print(f"The table: {query.split(' ')[-1]} has {result[0]} rows ")
        except psycopg2.Error as e:
            print("Error running select tables")
            print(e)


def main():

    # create connection
    conn, cur = establish_connection()

    # run copy table
    copy_table(conn=conn, cur=cur, sql=copy_table_statement)

    # insert table statement
    insert_table(conn=conn, cur=cur, sql=insert_table_statement)

    # check if data inserted correctly
    quality_checks(cur=cur, sql=select_statement)


if __name__ == "__main__":
    main()