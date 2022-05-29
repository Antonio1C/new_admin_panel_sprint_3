from dotenv import load_dotenv
import os
import psycopg2


from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from extract_data import PGLoader
from status import Status

from contextlib import contextmanager


@contextmanager
def pg_context(**dsl):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield conn
    finally:
        conn.close()


def transfer_data(pg_conn):
    pg_loader = PGLoader(pg_conn)
    status = Status()
    status.connect()
    for movies in pg_loader.get_movies_from_database(status.get_status('mod_date')):
        print(len(movies))

    status.disconnect()
    return

if __name__ == '__main__':

    load_dotenv()

    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        }

    sqlite_path = os.environ.get('DB_SQLITE_PATH')
    with pg_context(**dsl) as pg_conn:
        transfer_data(pg_conn)