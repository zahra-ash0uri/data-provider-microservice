from dao.mongo.mongo_adapter import MongoAdapter
from dao.redis.redis_adapter import RedisAdapter
from tornado import gen


mongo_adapter = MongoAdapter()
redis_adapter = RedisAdapter()

def cache():
    """
    Callback function for caching documents from mongo to redis
    :return: cache function as generator
    """
    def do_cache():
        documents = mongo_adapter.retrieve_all()
        for doc in documents:
            redis_adapter.set_key(ad_id=doc['ad_id'], value=doc['ad_ctr'])

    return do_cache()

