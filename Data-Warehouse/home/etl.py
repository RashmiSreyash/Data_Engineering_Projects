import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
   """ 
   Load data from S3 into staging tables using COPY commands.
   """
    for query in copy_table_queries:
        print("RUNNING QUERY:\n", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into final analytics tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main ETL function:
    - Connects to Redshift
    - Loads staging tables
    - Inserts data into final tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("DEBUG ARN FROM sql_queries:", config['IAM_ROLE']['ARN'])

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()