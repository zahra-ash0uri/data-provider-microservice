from pymongo import MongoClient
from config.runtime_config import RuntimeConfig


class MongoConnectorSingleton:
    __conn__ = None

    host = RuntimeConfig.MONGODB_HOST
    port = RuntimeConfig.MONGODB_PORT

    @classmethod
    def create_connection(cls):
        if not cls.__conn__:
            cls.__conn__ = MongoClient(f'{cls.host}:{cls.port}')
        return cls.__conn__

    """ For explicitly opening database connection """
    def __enter__(self):
        self.__conn__ = self.create_connection()
        return self.__conn__

    def __exit__(self):
        self.__conn__.close()

