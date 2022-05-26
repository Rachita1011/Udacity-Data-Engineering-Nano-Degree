import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    The purpose of this function is to drop any existing tables in the Redshift cluster.
        :param cur: cursor object to the database connection
        :param conn: connection object to the database
        
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    The purpose of this function is to create the tables in the Redshift cluster.
        :param cur: cursor object to the database connection
        :param conn: connection object to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function connects to the redshift cluster and resets all the tables in the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()