import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Executes all the drop commands in the drop
        table queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates all the tables in the database schema
        on the Redshift cluster
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Reads all the configuration parameters from the dwh.cfg file.
        Connects to the database on redshift.
        Drops all the tables if they exists.
        Creates all the tables.
        Closes the connection.
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