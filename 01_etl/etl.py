from time import sleep
from dotenv import load_dotenv
from extract_data import PGLoader
from load_data import ESLoader
from status import Status
from datetime import datetime
import time


def transfer_data():
    pg_loader = PGLoader()
    es_loader = ESLoader()
    status = Status()
    status.connect()

    mod_date = status.get_status('mod_date')
    mod_date = datetime.fromtimestamp(mod_date) if mod_date else None

    new_date = time.time()

    for movies in pg_loader.get_movies_from_database(mod_date):
        es_loader.save_data(movies)

    status.set_status('mod_date', new_date)
    status.disconnect()

if __name__ == '__main__':

    load_dotenv()
    while True:
        transfer_data()
        sleep(1)