import pymysql
import csv
import logging

from io import StringIO

class GoogleCloudSQL:

    def __init__(self, server, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        try:
            self.conn = pymysql.connect(host=self.server, user=self.user, password=self.password, database=self.database)
            return True
        except Exception as e:
            logging.error(f"Connection error: {e}")
            return False

    def close(self):
        self.conn.close()

    def execute_query(self, query):
        logging.info(f'Executing Query: {query}')
        results = []
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                if len(results) == 0:
                    logging.debug("0 rows returned")
                    return []

                headers = [column[0] for column in cursor.description]
                results.insert(0, tuple(headers))

            return results

        except Exception as e:
            logging.error(f"Query error: {e}")
            return []

    def process_table_string(self, input_str):
        items = input_str.split(',')
        items = [item.split('.')[-1] for item in items]
        formatted_str = "', '".join(items)
        result = f"'{formatted_str}'"
        return result

    def execute_schema(self, table_list):
        queryPart = self.process_table_string(table_list)
        return f"SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME, ', ', COLUMN_NAME, ', ', DATA_TYPE) AS 'Table, Column, DataType' FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN ({queryPart})"
