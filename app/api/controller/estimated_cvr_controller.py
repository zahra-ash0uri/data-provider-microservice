from tornado.web import RequestHandler
from dao.redis.redis_adapter import RedisAdapter
from dao.mongo.mongo_adapter import MongoAdapter
from vo.app_vo import AppVO
import json
import time

redis_adapter = RedisAdapter()
mongo_adapter = MongoAdapter()


class EstimatedCVRController(RequestHandler):

    def post(self):
        received_at = time.time()

        body = self.request.body.decode(encoding='utf-8')

        ad_ids = []
        try:
            ad_ids = json.loads(body).get(AppVO.RequestBody.add_ids, [])
        except:
            self.terminate_request(400, AppVO.ErrorMessages.bad_input, received_at)

        if ad_ids:
            for ad_id in ad_ids:
                if ad_id not in mongo_adapter.distinct_ads:
                    self.terminate_request(400, AppVO.ErrorMessages.invalid_input, received_at)

            try:
                response = self.get_keys(ad_ids)
                self.write({
                    AppVO.ResponseBody.data: response
                })
                self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)
            except Exception as e:
                if str(e) == AppVO.ErrorMessages.service_unavailable:
                    self.terminate_request(503, str(e), received_at)

                elif str(e) == AppVO.ErrorMessages.wait:
                    self.terminate_request(429, str(e), received_at)

                else:
                    self.terminate_request(500, AppVO.ErrorMessages.unknown_issue, received_at)

    @staticmethod
    def get_keys(ad_ids):
        resp = []

        def get_key(x):
            while True:
                try:
                    v = redis_adapter.get_key(x)
                    return v
                except:
                    raise Exception(AppVO.ErrorMessages.service_unavailable)

        for ad_id in ad_ids:
            value = None

            try:
                value = get_key(ad_id)
            except Exception as e:
                raise e

            if value:
                resp.append({
                    AppVO.MongodbCollectionFields.ad_id: ad_id,
                    AppVO.MongodbCollectionFields.ad_ctr: float(value)
                })
            else:
                raise Exception(AppVO.ErrorMessages.wait)

        return resp

    @staticmethod
    def update_system_requests_status(received_at, response_time):
        mongo_adapter.insert_new_stat(received_at=received_at, response_time=response_time)

    def terminate_request(self, status_code, message, received_at):
        self.set_status(status_code)
        self.finish({AppVO.ResponseBody.message: message})
        self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)
