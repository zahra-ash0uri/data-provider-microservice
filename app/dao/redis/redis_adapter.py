from dao.redis.redis_connector_singleton import RedisConnectorSingleton
from config.runtime_config import RuntimeConfig


class RedisAdapter:
    """
        A class to have transactions with redis database
    """

    __connection__ = None
    key = RuntimeConfig.CACHING_KEY_NAME

    def __init__(self):
        self.get_connection()

    @classmethod
    def get_connection(cls, new: bool = False):
        """Get connection singleton

        :param new: flag indicates to create new connection singleton
        :return: class instance collection
        """

        if new or not cls.__connection__:
            cls.__connection__ = RedisConnectorSingleton().create_connection()
        return cls.__connection__

    def set_key(self, ad_id: int, value: float):
        key = self.key.format(ad_id)
        res = self.__connection__.set(name=key, value=value, ex=60)
        return res

    def get_key(self, ad_id: int):
        key = self.key.format(ad_id)
        try:
            value = self.__connection__.get(key)
            if value:
                return value
        except Exception as e:
            raise Exception(f'Redis can not retrieve a document with ad_id {ad_id}, {str(e)}')


if __name__ == '__main__':
    import time
    while True:
        a = RedisAdapter()
        b = RedisAdapter()
        print(id(a.__connection__))
        print(id(b.__connection__))
        time.sleep(2)

    # r = a.set_value(ad_id=100, value=0.55)
    # print(r)
    # r = a.get_value(ad_id=1)
    # print(r, type(r))
    # x = float(r)
    # print(x, type(x))

