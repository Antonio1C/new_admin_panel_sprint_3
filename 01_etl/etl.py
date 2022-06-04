from time import sleep
import uuid
from dotenv import load_dotenv
from extract_data import PGLoader
from load_data import ESLoader
from status import Status


def transfer_data():
    pg_loader = PGLoader()
    es_loader = ESLoader()
    status = Status()
    status.connect()

    for movies in pg_loader.get_movies_from_database(status.get_status('mod_date')):
        es_loader.save_data(movies)

    status.disconnect()
    return

if __name__ == '__main__':

    load_dotenv()
    while True:
        transfer_data()
        sleep(1)