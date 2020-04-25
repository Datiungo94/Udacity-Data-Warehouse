import psycopg2
import psycopg_connect
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables

    Args:
        cur: cursor for Redshift database
        conn: connection to Redsfhit database
    Return:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables to facts and dimension tables

    Args:
        cur: cursor for Redshift database
        conn: connection to Redsfhit database
    Return:
        None
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Flow of the ETL:
    1) Load data from S3
    2) Extract and transform data into facts and dimension tables
    """
    cur, conn = psycopg_connect.connect()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()