from tornado.httpserver import HTTPServer
from tornado.options import define
from tornado.web import Application
from tornado.ioloop import IOLoop
from api.view.get_estimated_cvr import GetEstimatedCVR
from config.runtime_config import RuntimeConfig


define('port', default=RuntimeConfig.APP_PORT, help='port to listen on')


def create_and_start_app():
    app = Application([
        ('/predict', GetEstimatedCVR)
    ],
        debug=RuntimeConfig.DEBUG_MODE)
    http_server = HTTPServer(app)
    http_server.listen(RuntimeConfig.APP_PORT)
    print(f'Listening on http:/{RuntimeConfig.APP_HOST}:{RuntimeConfig.APP_PORT}')
    IOLoop.current().start()



