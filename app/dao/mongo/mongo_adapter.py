from dao.mongo.mongo_connector_singleton import MongoConnectorSingleton
from config.runtime_config import RuntimeConfig


class MongoAdapter:
    connection = None
    key = RuntimeConfig.CACHING_KEY_NAME

    def __init__(self):
        self.get_connection()
        self.db = self.connection.tapsell
        self.collection = self.db.tapsell_ctr


    @classmethod
    def get_connection(cls, new: bool = False):
        if new or not cls.connection:
            cls.connection = MongoConnectorSingleton().create_connection()
        return cls.connection

    def insert_many(self, documents: list):
        try:
            self.collection.insert_many(documents=documents)
        except Exception as e:
            raise Exception(f'Document initial insertions failed!, {str(e)}')

    def create_index_on_field(self, filed: str):
        try:
            self.collection.create_index(keys=filed)
        except Exception as e:
            raise Exception(f'Index creation failed!, {str(e)}')

    def find_one(self, ad_id: int):
        res = self.collection.find_one({"ad_id": ad_id})
        return res

    @property
    def distinct_ads(self):
        return self.collection.distinct(key='ad_id')

    def retrieve_all(self):
        distinct = self.distinct_ads

        # TODO : We assume the db collection is normalized, with no redundancy
        res = self.collection.find({
            "ad_id": {
                "$in": distinct
            }
        })
        return res


if __name__ == '__main__':
    a = MongoAdapter()
    c = a.retrieve_all()
    # cnt = 0
    # for d in c:
    #     print(d)
    #     cnt += 1
    # print(cnt)