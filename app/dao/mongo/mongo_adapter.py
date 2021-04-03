from dao.mongo.mongo_connector_singleton import MongoConnectorSingleton
from config.runtime_config import RuntimeConfig
import pandas as pd


class MongoAdapter:
    """
    A class to have transactions with mongodb
    """

    __connection__ = None

    def __init__(self):
        self.get_connection()
        self.db = self.__connection__.tapsell
        self.ad_ctr_collection = self.db.ad_ctr_collection
        self.system_requests_stats_collection = self.db.system_requests_stats_collection

    @classmethod
    def get_connection(cls, new: bool = False):
        """Get connection singleton

        :param new: flag indicates to create new connection singleton
        :return: class instance collection
        """
        if new or not cls.__connection__:
            cls.__connection__ = MongoConnectorSingleton().create_connection()
        return cls.__connection__

    def insert_many(self, documents: list) -> None:
        try:
            self.ad_ctr_collection.insert_many(documents=documents)
        except Exception as e:
            raise Exception(f'Document initial insertions failed!, {str(e)}')

    def create_index_on_field(self, field: str) -> None:
        """ Create index on a field in ad_ctr_collection

        usage: invoked after db initialization
        :param field: string
        :return:
        """
        try:
            self.ad_ctr_collection.create_index(keys=field)
        except Exception as e:
            raise Exception(f'Index creation failed!, {str(e)}')

    def find_one(self, ad_id: int):
        """Query database to find document by field ad_id

        :param ad_id: int
        :return: mongodb cursor object
        """
        res = self.ad_ctr_collection.find_one({"ad_id": ad_id})
        return res

    def insert_new_stat(self, received_at: int, response_time: float) -> None:
        try:
            self.system_requests_stats_collection.insert({
                "received_at": received_at,
                "response_time": response_time
            })
        except Exception as e:
            raise Exception(f'Document insertion failed!, {str(e)}')

    @property
    def distinct_ads(self) -> list:
        """Class property to fetch distinct documents by ad_id

        :return: list of documents
        """
        try:
            return self.ad_ctr_collection.distinct(key='ad_id')
        except:
            raise Exception('Database Not Available!')

    @property
    def system_requests_stats_dataframe(self):
        """Class property to fetch db collection and convert it to pandas dataframe

        :return: pandas.DataFrame
        """
        try:
            cursor = self.system_requests_stats_collection.find({})
            df = pd.DataFrame(list(cursor))
            return df
        except:
            raise Exception('Database Not Available!')

    @property
    def count_system_requests_stats_collection(self) -> int:
        """Class property to count documents in collection

        :return: int
        """
        try:
            cursor = self.system_requests_stats_collection.find({})
            return cursor.count()
        except:
            raise Exception('Database Not Available!')

    def retrieve_all(self):
        """ Retrieve distinct documents

        :return: db cursor object
        """

        try:
            distinct = self.distinct_ads
            # TODO : We assume the db collection is normalized, with no redundancy

            res = self.ad_ctr_collection.find({
                "ad_id": {
                    "$in": distinct
                }
            })
            return res
        except:
            raise Exception('Database Not Available!')