import os

from typing import Any
from redis import Redis
from dotenv import load_dotenv

class Status():

    def connect(self) -> None:
        load_dotenv()
        host = os.environ.get('REDIS_HOST', 'localhost')
        port = os.environ.get('REDIS_PORT', 6379)
        self.__redis_db = Redis(host=host, port=port)

    def disconnect(self) -> None:
        self.__redis_db.close()

    def get_status(self, key) -> Any:
        self.__redis_db.set_response_callback('GET', float)
        return self.__redis_db.get(key)

    def set_status(self, key: str, value: Any) -> None:
        self.__redis_db.set(key, value=value)