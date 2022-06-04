import os
from elasticsearch import Elasticsearch, helpers

from backoff import backoff


class ESLoader:
    def __init__(self) -> None:
        hosts = [
            {
                'host': os.environ.get('HOST_ES', 'localhost'),
                'port': os.environ.get('PORT_ES', 9200),
            }
        ]
        self.__client = Elasticsearch(hosts=hosts)

    def save_data(self, data):
        self.__check_connection()
        helpers.bulk(self.__client, self.__generate_data(data))
    
    @backoff((ConnectionError, ))
    def __check_connection(self):
        if not self.client.ping():
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
            doc = {}
            for fld_name in self.__get_fields():
                if fld_name in persons_fields:
                    doc[fld_name] = self.__get_persons_list(self, movie[fld_name])
                elif fld_name == 'director' and movie[fld_name] is None:
                    doc[fld_name] = []
                else: doc[fld_name] = movie[fld_name]

            yield {
                '_index': 'movies',
                '_id': movie['id'],
                **doc,
            }
