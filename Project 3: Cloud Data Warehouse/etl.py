import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads the data from all the logs by using all the copy table queries into the staging tables.
    :param cur: the database cursur
    :param conn:the database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts the data from the staging tables into the five tables designed as part of the star schema.
    :param cur: the database cursur
    :param conn:the database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is where we're actually calling the functions insert_tables and load_staging_tables to run. Connection details are specified as part of dwh.cfg file.  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()