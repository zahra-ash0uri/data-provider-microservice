from dao.mongo.mongo_adapter import MongoAdapter
import csv


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
    mongo_adapter = MongoAdapter()
    documents = read_and_prepare_records()
    mongo_adapter.insert_many(documents)
    mongo_adapter.create_index_on_field(filed='ad_id')
    print("Initialized successfully!")


if __name__ == '__main__':
    init_db()