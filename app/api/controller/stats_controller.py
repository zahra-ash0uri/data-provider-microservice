from tornado.web import RequestHandler
from dao.mongo.mongo_adapter import MongoAdapter
from vo.app_vo import AppVO
from datetime import datetime
import json
import time

mongo_adapter = MongoAdapter()


class StatsController(RequestHandler):

    def get(self):
        received_at = time.time()

        stats_dataframe = None
        try:
            stats_dataframe = mongo_adapter.system_requests_stats_dataframe
        except:
            self.terminate_request(status_code=503, message=AppVO.ErrorMessages.service_unavailable,
                                   received_at=received_at)

        if stats_dataframe is not None:
            totals = mongo_adapter.count_system_requests_stats_collection
            average = float(stats_dataframe[AppVO.MongodbCollectionFields.response_time].mean()) \
                if not stats_dataframe[AppVO.MongodbCollectionFields.response_time].isnull().values.all() else None
            percentile = float(stats_dataframe[AppVO.MongodbCollectionFields.response_time].quantile(q=0.99)) \
                if not stats_dataframe[AppVO.MongodbCollectionFields.response_time].isnull().values.all() else None

            self.send_response(totals, average, percentile)
            self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)

        # Condition which the first incoming request to system is stats/
        else:
            self.handle_if_first_incoming_request(received_at)

    def post(self):
        received_at = time.time()
        body = self.request.body.decode(encoding='utf-8')

        from_ = None
        to_ = None
        from_timestamp = None
        to_timestamp = None

        try:
            body = json.loads(body)
            from_ = body[AppVO.RequestBody.from_]
            to_ = body[AppVO.RequestBody.to_]

        except:
            self.terminate_request(400, AppVO.ErrorMessages.bad_input, received_at)

        try:
            from_timestamp = datetime.strptime(from_, AppVO.RequestBody.standard_datetime_format).timestamp()
            to_timestamp = datetime.strptime(to_, AppVO.RequestBody.standard_datetime_format).timestamp()
        except:
            self.terminate_request(400, AppVO.ErrorMessages.invalid_input, received_at)

        stats_dataframe = None
        try:
            stats_dataframe = mongo_adapter.system_requests_stats_dataframe
        except:
            self.terminate_request(status_code=503, message=AppVO.ErrorMessages.service_unavailable,
                                   received_at=received_at)

        if stats_dataframe is not None:
            filter = (from_timestamp <= stats_dataframe[AppVO.MongodbCollectionFields.received_at])\
                     & (stats_dataframe[AppVO.MongodbCollectionFields.received_at] < to_timestamp)
            stats_dataframe = stats_dataframe[filter]

            totals = int(stats_dataframe[AppVO.MongodbCollectionFields.received_at].count())\
                if not stats_dataframe[AppVO.MongodbCollectionFields.received_at].isnull().values.all() else 0
            average = float(stats_dataframe[AppVO.MongodbCollectionFields.response_time].mean())\
                if not stats_dataframe[AppVO.MongodbCollectionFields.response_time].isnull().values.all() else None
            percentile = float(stats_dataframe[AppVO.MongodbCollectionFields.response_time].quantile(q=0.99))\
                if not stats_dataframe[AppVO.MongodbCollectionFields.response_time].isnull().values.all() else None

            self.send_response(totals, average, percentile)
            self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)
        else:
            self.handle_if_first_incoming_request(received_at)

    def handle_if_first_incoming_request(self, received_at):
        totals = 0
        average = None
        percentile = None

        self.send_response(totals, average, percentile)
        self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)

    def send_response(self, totals, average, percentile):
        response = [{
            AppVO.ResponseBody.total_received_requests: totals,
            AppVO.ResponseBody.average_response_time: average,
            AppVO.ResponseBody.response_time_99percentile: percentile
        }]
        self.write({
            AppVO.ResponseBody.data: response
        })

    def terminate_request(self, status_code, message, received_at):
        self.set_status(status_code)
        self.finish({AppVO.ResponseBody.message: message})
        self.update_system_requests_status(received_at, time.time() - received_at)

    @staticmethod
    def update_system_requests_status(received_at, response_time):
        mongo_adapter.insert_new_stat(received_at=received_at, response_time=response_time)
