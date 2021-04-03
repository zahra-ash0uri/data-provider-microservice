from tornado.web import RequestHandler
from dao.mongo.mongo_adapter import MongoAdapter
from app.vo.app_vo import AppVO
from datetime import datetime
from plotly.subplots import make_subplots
import plotly.express as px
import io
import time

mongo_adapter = MongoAdapter()
buffer = io.StringIO()
fig = make_subplots(rows=3, cols=1)


class Dashboard(RequestHandler):
    def get(self):
        received_at = time.time()

        try:
            df1, df2, df3 = self.create_metrics_values()
            self.create_and_convert_three_figures_to_one(df1, df2, df3)
            fig.write_html(buffer)
            html = buffer.getvalue()

            self.write(html)
            self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)
        except:
            self.terminate_request(503, AppVO.ErrorMessages.service_unavailable, received_at)


    @staticmethod
    def create_metrics_values():
        try:
            df = mongo_adapter.system_requests_stats_dataframe
            received_at = df['received_at']
            counter = []
            avg = []
            quantile = []
            for timestamp in received_at:
                df = df[df['received_at'] < timestamp]
                if not df['received_at'].isnull().values.all():
                    counter.append(df['received_at'].count())
                    avg.append(df['response_time'].mean())
                    quantile.append(df['response_time'].quantile(q=0.99))
                else:
                    counter.append(0)
                    avg.append(0)
                    quantile.append(0)

            x_axis = list(map(lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S"), received_at))

            counter_dataframe = {
                "datetime": x_axis,
                "total_received_requests": counter
            }
            avg_dataframe = {
                "datetime": x_axis,
                "average_response_time": avg
            }

            quantile_dataframe = {
                "datetime": x_axis,
                "99_percentile_response_time": quantile
            }

            return counter_dataframe, avg_dataframe, quantile_dataframe
        except Exception as e:
            raise e

    @staticmethod
    def create_and_convert_three_figures_to_one(df1: dict, df2: dict, df3: dict):
        fig1 = px.bar(df1, x="datetime", y="total_received_requests")
        fig1.update_xaxes(type='category')
        bar1 = fig1['data'][0]
        bar1['name'] = "Total Received Requests(second)"
        bar1['marker']['color'] = '#44b87c'
        bar1['showlegend'] = True

        fig2 = px.bar(df2, x="datetime", y="average_response_time", title="Average Response Time")
        fig2.update_xaxes(type='category')
        bar2 = fig2['data'][0]
        bar2['name'] = "Average Response Time(second)"
        bar2['marker']['color'] = '#d964aa'
        bar2['showlegend'] = True

        fig3 = px.bar(df3, x="datetime", y="99_percentile_response_time")
        fig3.update_xaxes(type='category')
        bar3 = fig3['data'][0]
        bar3['name'] = "99Percentile Response Time(second)"
        bar3['marker']['color'] = '#14b0db'
        bar3['showlegend'] = True

        fig.add_trace(bar1, row=1, col=1)
        fig.add_trace(bar2, row=2, col=1)
        fig.add_trace(bar3, row=3, col=1)

    @staticmethod
    def update_system_requests_status(received_at, response_time):
        mongo_adapter.insert_new_stat(received_at=received_at, response_time=response_time)

    def terminate_request(self, status_code, message, received_at):
        self.set_status(status_code)
        self.finish({AppVO.ResponseBody.message: message})
        self.update_system_requests_status(received_at=received_at, response_time=time.time() - received_at)