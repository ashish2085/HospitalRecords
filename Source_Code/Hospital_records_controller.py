import pandas as pd
from pyhive import hive
class HiveConnection:
    @staticmethod
    def select_query(query_str: str, database:str =HIVE_SCHEMA) -> pd.DataFrame:
        """
        Execute a select query which returns a result set
        :param query_str: select query to be executed
        :param database: Hive Schema
        :return:
        """
        conn = hive.Connection(host=HIVE_URL, port=HIVE_PORT, database=database, username=HIVE_USER)

        try:
            result = pd.read_sql(query_str, conn)
            return result
        finally:
            conn.close()

    @staticmethod
    def execute_query(query_str: str, database: str=HIVE_SCHEMA):
        """
        execute an query which does not return a result set.
        ex: INSERT, CREATE, DROP, ALTER TABLE
        :param query_str: Hive query to be executed
        :param database: Hive Schema
        :return:
        """
        conn = hive.Connection(host=HIVE_URL, port=HIVE_PORT, database=database, username=HIVE_USER)
        cur = conn.cursor()
        # Make sure to set the staging default to HDFS to avoid some potential S3 related errors
        cur.execute("SET hive.exec.stagingdir=/tmp/hive/")
        cur.execute("SET hive.exec.scratchdir=/tmp/hive/")
        try:
            cur.execute(query_str)
            return "SUCCESS"
        finally:
            conn.close()