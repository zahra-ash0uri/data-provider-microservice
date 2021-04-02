from redis import Redis
from config.runtime_config import RuntimeConfig


class RedisConnectorSingleton:
    __conn__ = None
    host = RuntimeConfig.REDIS_HOST
    port = RuntimeConfig.REDIS_PORT
    database = RuntimeConfig.REDIS_DATABASE
    username = RuntimeConfig.REDIS_USERNAME
    password = RuntimeConfig.REDIS_PASSWORD

    @classmethod
    def create_connection(cls):
        cls.__conn__ = Redis(
            host=cls.host,
            port=cls.port,
            # username=cls.username,
            # password=cls.password,
            db=cls.database
        )
        return cls.__conn__

    """ For explicitly opening database connection """
    def __enter__(self):
        self.__conn__ = self.create_connection()
        return self.__conn__

    def __exit__(self):
        self.__conn__.close()

