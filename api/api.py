import os
import urllib.parse as up
from typing import List, Tuple, Optional
import json

import flask
import psycopg2
from flask import request


class PostgresManager:

    DEC = ', '
    DATABASE_URL = 'postgres://rduzihbd:wH5rZa8WqYsA6ILOBFV0DrUp-xiFO9yA@kandula.db.elephantsql.com:5432/rduzihbd'

    def __init__(self):
        up.uses_netloc.append("postgres")
        # url = up.urlparse(os.environ["DATABASE_URL"])
        url = up.urlparse(PostgresManager.DATABASE_URL)
        self.conn = psycopg2.connect(database=url.path[1:], user=url.username, password=url.password, host=url.hostname, port=url.port)

    def __del__(self):
        self.conn.close()

    def _execute_query(self, qur_str):
        with self.conn.cursor() as curs:
            curs.execute(qur_str)
            return_tuple = curs.fetchall()
        print(return_tuple)
        return return_tuple

    def get_columns_from_table(self, table: str, columns: List[str]) -> Tuple[str]:
        list_columns_str = PostgresManager.DEC.join(columns)
        query_str = f'SELECT {list_columns_str} FROM {table}'
        return self._execute_query(query_str)

    def get_all_table(self, table: str) -> Tuple[str]:
        query_str = f'SELECT * FROM {table}'
        return self._execute_query(query_str)

    def query(self, table: str, columns: Optional[List[str]] = None) -> str:
        if columns:
            tuple_results = self.get_columns_from_table(table, columns)
        else:
            tuple_results = self.get_columns_from_table(table)
        json_str = json.dumps(tuple_results)
        # print(json_str)
        return json_str


P = PostgresManager()
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    '''
    Handle GET request for landing page with instruction for interface
    :return: string: Instructions to server API
    '''
    return "<h1>Distant Reading Archive</h1>" \
           "<p>This site is a prototype API for distant reading of science fiction novels.</p>"


@app.route('/get_all_table', methods=['GET'])
def get_all_table():
    '''
    Handle GET request to execute SQL request to DB to get all table rows.
    :param: get table parameter from URL query parameter "table"
    :return: string: JSON for returned rows from SQL query
    '''
    if 'table' in request.args:
        table = str(request.args['table'])
    else:
        return "Error: No id field provided. Please specify an id."

    return P.query(table)


@app.route('/get_data_from_table', methods=['GET'])
def get_data_from_table():
    '''
    Handle GET request to execute SQL request to DB to get list of columns from table.
    :param: get table parameter from URL query parameter "table"
    :param: get list of columns from URL query parameter "columns" as string with comma delimiter
    :return: string: JSON for returned rows from SQL query
    '''
    if 'table' in request.args:
        if 'columns' in request.args:
            table = str(request.args['table'])
            columns_str = str(request.args['columns'])
            columns_list = columns_str.split(',')
    else:
        return "Error: No id field provided. Please specify an id."

    return P.query(table, columns_list)


@app.route('/post_from_table', methods=['POST'])
def post_from_table():
    '''
    Handle POST request to execute SQL request to DB to get list of columns from table.
    :param: get JSON string from body query with table string parameter and columns list of string
    :return: string: JSON for returned rows from SQL query
    '''
    if request.data:
        body_str = str(request.data.decode("utf-8"))
        body_dict = json.loads(body_str)
    else:
        return "Error: No id field provided. Please specify an id."

    return P.query(body_dict['table'], body_dict['columns'])


app.run()
