import os

from typing import Any
from redis import Redis
from dotenv import load_dotenv

class Status():

    def connect(self) -> None:
        load_dotenv()
        host = os.environ('REDIS_HOST', 'localhost')
        port = os.environ.get('REDIS_PORT', 6379)
        self.redis_db = Redis(host=host, port=port)

    def get_status(self, key) -> Any:
        return self.redis_db.get(key)

    def set_status(self, key: str, value: Any) -> None:
        self.redis_db.set(key, value=value)