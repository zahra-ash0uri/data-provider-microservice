from dao.mongo.mongo_adapter import MongoAdapter
import csv

mongo_adapter = MongoAdapter()


def read_and_prepare_records(filename='./task-23-dataset.csv'):
    input_file = csv.reader(open(filename, 'r'))
    input_file = list(input_file)
    docs = list()
    for row in input_file[1:]:
        docs.append({
            'ad_id': int(row[0]),
            'ad_ctr': float(row[1])
        })
    return docs


def init_db():
    """ initializes mongodb with records in csv file

    :return: print acknowledgment that task completed
    """

    documents = read_and_prepare_records()
    mongo_adapter.insert_many(documents)
    mongo_adapter.create_index_on_field(field='ad_id')
    print("Initialized successfully!")


if __name__ == '__main__':

    """ 
    usage: invoke init_db to do initialize db
    """

    init_db()