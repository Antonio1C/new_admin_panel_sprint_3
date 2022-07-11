import os
from typing import Dict, List
from elasticsearch import Elasticsearch, helpers

from pydantic import BaseModel

from backoff import backoff

class Movie(BaseModel):
    id: str
    imdb_rating: float | None
    genre: List[str]
    title: str
    description: str | None
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    writers: Dict
    actors: Dict

class ESLoader:
    def __init__(self) -> None:
        hosts = [
            {
                'host': os.environ.get('ES_HOST', 'localhost'),
                'port': int(os.environ.get('ES_PORT', 9200)),
            }
        ]
        self.__client = Elasticsearch(hosts=hosts)

    def save_data(self, data) -> None:
        self.__check_connection()
        helpers.bulk(self.__client, self.__generate_data(data))
    
    @backoff((ConnectionError, ))
    def __check_connection(self) -> None:
        if not self.__client.ping():
            raise ConnectionError

    def __get_fields(self) -> list:
        return [
            'id',
            'imdb_rating',
            'genre',
            'title',
            'description',
            'director',
            'actors_names',
            'writers_names',
            'writers',
            'actors',
        ]

    def __get_persons_list(self, persons: list[dict]) -> list[dict]:
        return [
            {'id': person['id'], 'name': person['name']} \
                for person in persons
        ]

    def __generate_data(self, movies_list):
        persons_fields = ['actors', 'writers']
        for movie in movies_list:
            doc = Movie(**movie)
            doc.writers = self.__get_persons_list(movie['writers'])
            doc.actors = self.__get_persons_list(movie['actors'])

            yield {
                '_index': 'movies',
                '_id': movie['id'],
                **doc.dict(),
            }
