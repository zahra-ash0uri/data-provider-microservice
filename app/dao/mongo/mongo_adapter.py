from dao.mongo.mongo_connector_singleton import MongoConnectorSingleton
from config.runtime_config import RuntimeConfig
import pandas as pd

class MongoAdapter:
    connection = None
    key = RuntimeConfig.CACHING_KEY_NAME

    def __init__(self):
        self.get_connection()
        self.db = self.connection.tapsell
        self.ad_ctr_collection = self.db.ad_ctr_collection
        self.system_requests_status_collection = self.db.system_requests_status_collection

    @classmethod
    def get_connection(cls, new: bool = False):
        if new or not cls.connection:
            cls.connection = MongoConnectorSingleton().create_connection()
        return cls.connection

    def insert_many(self, documents: list):
        try:
            self.ad_ctr_collection.insert_many(documents=documents)
        except Exception as e:
            raise Exception(f'Document initial insertions failed!, {str(e)}')

    def create_index_on_field(self, filed: str):
        try:
            self.ad_ctr_collection.create_index(keys=filed)
        except Exception as e:
            raise Exception(f'Index creation failed!, {str(e)}')

    def find_one(self, ad_id: int):
        res = self.ad_ctr_collection.find_one({"ad_id": ad_id})
        return res

    def insert_new_status(self, received_at, response_time):
        try:
            self.system_requests_status_collection.insert({
                "received_at": received_at,
                "response_time": response_time
            })
        except Exception as e:
            raise Exception(f'Document insertion failed!, {str(e)}')

    @property
    def distinct_ads(self):
        return self.ad_ctr_collection.distinct(key='ad_id')

    @property
    def system_requests_status_dataframe(self):
        cursor = self.system_requests_status_collection.find({})
        df = pd.DataFrame(list(cursor))
        return df

    def retrieve_all(self):
        distinct = self.distinct_ads

        # TODO : We assume the db collection is normalized, with no redundancy
        res = self.ad_ctr_collection.find({
            "ad_id": {
                "$in": distinct
            }
        })
        return res


if __name__ == '__main__':
    import pandas as pd
    from datetime import datetime
    import json

    a = MongoAdapter()
    # d = a.system_requests_status_dataframe
    # print(d, '\n*******')
    # fr = 1.617023 * (10**9)
    # to = 1.617037 * (10**9)
    #
    # d2 = d[(fr <= d["received_at"]) & (d["received_at"] < to)]
    # print(d2)
    # print("count", d['received_at'].count())
    # print("avg", d['response_time'].mean())
    # print("99 percentile", d['response_time'].quantile(q=0.99))

    # c = a.retrieve_all()
    # cnt = 0
    # for d in c:
    #     print(d)
    #     cnt += 1
    # print(cnt)