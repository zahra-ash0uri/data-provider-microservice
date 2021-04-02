from dao.mongo.mongo_adapter import MongoAdapter
from dao.redis.redis_adapter import RedisAdapter
from config.runtime_config import RuntimeConfig
from tornado import gen
import time

mongo_adapter = MongoAdapter()
redis_adapter = RedisAdapter()


@gen.coroutine
def caching():

    def cache():
        while True:
            try:
                documents = mongo_adapter.retrieve_all()
                for doc in documents:
                    redis_adapter.set_key(ad_id=doc['ad_id'], value=doc['ad_ctr'])

                gen.sleep(RuntimeConfig.CACHING_INTERVAL)
            except Exception as e:
                print(str(e))
                time.sleep(3)
                continue

    return cache()

