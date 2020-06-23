import configparser
import psycopg2
import boto3
import botocore
import sys
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn, date_time):
    """
    Loads data from S3 to the staging tables
    """
    for query in copy_table_queries:
        query = query.format(date_time)
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads the insert table queries for staging as well as the schema tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def check_folder(date_time):
    """
    Checks if the new daily folder is generated based on current date prefix
    """
    s3 = boto3.client('s3')
    result = s3.list_objects(Bucket="daily-transaction",
                             Prefix=date_time + "/transaction_")
    if "Contents" in result:
        print("Folder Exists")
        print(result)
        return True
    else:
        print("Folder does not Exists")
        print(result)
        return False


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    now = datetime.now()
    date_time = now.strftime("%Y%m%d")

    if check_folder(date_time):
        load_staging_tables(cur, conn, date_time)
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
