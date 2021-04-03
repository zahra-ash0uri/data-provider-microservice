from tornado.httpserver import HTTPServer
from tornado.options import define
from tornado.web import Application
from tornado.ioloop import IOLoop
from tornado import gen
from api.controller.estimated_cvr_controller import EstimatedCVRController
from api.controller.stats_controller import StatsController
from api.controller.dashboard import Dashboard
from config.runtime_config import RuntimeConfig


define('port', default=RuntimeConfig.APP_PORT, help='port to listen on')


def create_and_start_app():
    app = Application([
        ('/predict', EstimatedCVRController),
        ('/stats', StatsController),
        ('/dashboard', Dashboard)
    ],
        debug=RuntimeConfig.DEBUG_MODE)
    http_server = HTTPServer(app)
    http_server.listen(RuntimeConfig.APP_PORT)
    print(f'Listening on http:/{RuntimeConfig.APP_HOST}:{RuntimeConfig.APP_PORT}')
    IOLoop.current().start()



