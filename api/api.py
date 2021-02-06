import os
import urllib.parse as up
from typing import List, Tuple, Optional
import json

import psycopg2
import flask
from flask import request


class PostgresManager:

    DEC = ', '

    def __init__(self):
        up.uses_netloc.append("postgres")
        url = up.urlparse(os.environ["DATABASE_URL"])
        self.conn = psycopg2.connect(database=url.path[1:], user=url.username, password=url.password, host=url.hostname, port=url.port)

    def __del__(self):
        self.conn.close()

    def _execute_query(self, qur_str):
        with self.conn.cursor() as curs:
            curs.execute(qur_str)
            return_tuple = curs.fetchone()
        print(return_tuple)
        return return_tuple

    def get_columns_from_table(self, table: str, columns: List[str]) -> Tuple[str]:
        list_columns_str = PostgresManager.DEC.join(columns)  # TODO: how to call to static element
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
        print(json_str)
        return json_str

P = PostgresManager()
P.query('bands', ['*'])
#
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"


@app.route('/get_data', methods=['GET'])
def get_all_data():
    return P.query('bands', ['*'])


@app.route('/get_data/table', methods=['GET'])
def get_data_from_table():
    # Check if an TABLE was provided as part of the URL.
    # If TABLE is provided, assign it to a variable.
    # If no TABLE is provided, display an error in the browser.
    if 'table' in request.args:
        table = str(request.args['table'])
    else:
        return "Error: No id field provided. Please specify an id."

    return P.query(table, ['*'])


app.run()
