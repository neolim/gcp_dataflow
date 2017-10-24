import psycopg2
from google.cloud import bigquery
from pprint import pprint
import json

# Settings
with open('../config.json') as json_conf:
    conf = json.load(json_conf)
    project = conf['project']
    postgres = conf['postgres']
    bq = conf['bigquery']

def load_data():
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (postgres['dbname'],
                                                                       postgres['user'],
                                                                       postgres['host'],
                                                                       postgres['pass']))
    cur = conn.cursor(name='serverSideCursor')
    cur.itersize = postgres['cur_itersize']

    bigquery_client = bigquery.Client(bq['project'])
    dataset = bigquery_client.dataset(bq['dataset'])
    table = dataset.table(bq['table'])

    # Reload table to get schema
    table.reload()
    # Get data from CloudSQL
    query = "" + postgres['query'] + ""
    cur.execute(query=query)

    batchSize = 10000
    while True:
        batch = cur.fetchmany(batchSize)
        if not batch:
            break
        errors = table.insert_data(batch)
        if not errors:
            print('Loaded {} rows into {}:{}'.format(batchSize, bq['dataset'], bq['table']))
        else:
            print('Errors:')
            pprint(errors)


if __name__ == '__main__':
    load_data()