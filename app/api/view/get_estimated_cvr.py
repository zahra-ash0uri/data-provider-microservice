from tornado.web import RequestHandler
from dao.redis.redis_adapter import RedisAdapter
from dao.mongo.mongo_adapter import MongoAdapter
import json

redis_adapter = RedisAdapter()
mongo_adapter = MongoAdapter()


class GetEstimatedCVR(RequestHandler):

    def post(self):
        body = self.request.body.decode(encoding='utf-8')

        ad_ids = []
        try:
            ad_ids = json.loads(body).get('adIdList', [])
        except:
            self.set_status(400)
            self.finish({"message": "Bad input format!"})
            return

        if ad_ids:
            for ad_id in ad_ids:
                if ad_id not in mongo_adapter.distinct_ads:
                    self.set_status(400)
                    self.finish({"message": "Invalid ad_id in input list, Check and try again!"})
                    return

            try:
                response = self.get_keys(ad_ids)
                self.write({
                    "data": response
                })
            except Exception as e:
                if str(e) == 'Service Unavailable!':
                    self.set_status(503)
                    self.finish({"message": str(e)})
                    return
                elif str(e) == 'Too Many Requests. Please Wait!':
                    self.set_status(429)
                    self.finish({"message": str(e)})
                    return
                else:
                    self.set_status(500)
                    self.finish({"message": str(e)})
                    return

    @staticmethod
    def get_keys(ad_ids):
        resp = []

        def get_key(x):
            while True:
                try:
                    v = redis_adapter.get_key(x)
                    return v
                except:
                    raise Exception('Service Unavailable!')

        for ad_id in ad_ids:
            value = None

            try:
                value = get_key(ad_id)
            except Exception as e:
                raise e

            if value:
                resp.append({
                    "ad_id": ad_id,
                    "ad_ctr": float(value)
                })
            else:
                raise Exception('Too Many Requests. Please Wait!')

        return resp
