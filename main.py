from api.create_and_start_app import create_and_start_app
from scripts.caching import caching
from tornado import ioloop


def keep_caching():
    yield caching()


def add_background_task():
    ioloop.IOLoop.current().spawn_callback(keep_caching)


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(add_background_task)
    create_and_start_app()