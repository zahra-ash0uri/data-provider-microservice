from api.create_and_start_app import create_and_start_app
from config.runtime_config import RuntimeConfig
from scripts.caching import cache
from tornado import ioloop


def start_service():
    p = ioloop.PeriodicCallback(cache, RuntimeConfig.CACHING_INTERVAL * 1000)
    p.start()
    create_and_start_app()


if __name__ == '__main__':

    while True:
        try:
            start_service()
        except:
            continue

