from typing import List
import uuid
from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch, helpers
import psycopg2
import requests


from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from extract_data import PGLoader
from status import Status

from contextlib import contextmanager

from dataclasses import dataclass, fields, field
from pydantic import BaseModel

@dataclass
class DataES:
    id: uuid.UUID
    imdb_rating: int
    genre: list[str]
    title: str
    description: str
    director: str
    actors_names: list[str]
    writers_names: list[str]
    writers: list[dict]
    actors: list[dict]

def get_fields(data_type: type) -> list:
    return [fld.name for fld in fields(data_type)]

def get_persons_list(persons: list[dict]) -> list[dict]:
    return [
        {'id': person['id'], 'name': person['name']} \
            for person in persons
    ]

@contextmanager
def pg_context(**dsl):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield conn
    finally:
        conn.close()


def generate_data(movies_list):
    persons_fields = ['actors', 'writers']
    for movie in movies_list:
        doc = {}
        for fld_name in get_fields(DataES):
            if fld_name in persons_fields:
                doc[fld_name] = get_persons_list(movie[fld_name])
            else: doc[fld_name] = movie[fld_name]

        # print(doc)
        yield {
            '_index': 'movies',
            '_id': movie['id'],
            **doc,
        }


def transfer_data(pg_conn: _connection):
    pg_loader = PGLoader(pg_conn)
    status = Status()
    status.connect()

    es_client = Elasticsearch('http://localhost:9200/')
    for movies in pg_loader.get_movies_from_database(status.get_status('mod_date')):
        helpers.bulk(es_client, generate_data(movies))

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