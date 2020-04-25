from sql_queries import create_table_queries, drop_table_queries
import psycopg_connect
import psycopg2


def drop_tables(cur, conn):
    """
    This query deletes existing tables if possible to ensure 
    error-free when our database creates new tables.

    Args:
        cur: cursor for Redshift database
        conn: connection to Redsfhit database
    Return:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging, facts, and dimension tables:
    1) staging_events
    2) staging_songs
    3) songplay
    4) users
    5) songs
    6) artists
    7) time
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Flow of create_tables.py
    1) Drop all tables before a new session
    2) Create new blank tables
    """
    cur, conn = psycopg_connect.connect()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()