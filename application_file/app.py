import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import bigquery
import os
import json

# Settings
with open('../config.json') as json_conf:
    conf = json.load(json_conf)
    project = conf['project']
    postgres = conf['postgres']
    bq = conf['bigquery']
    json_file = conf['file']


def export_postgres_to_json():
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (postgres['dbname'],
                                                                       postgres['user'],
                                                                       postgres['host'],
                                                                       postgres['pass']))
    cur = conn.cursor(name='serverSideCursor', cursor_factory=RealDictCursor)
    cur.itersize = postgres['cur_itersize']
    query = "" + postgres['query'] + ""
    cur.execute(query=query)

    with open(json_file['name'], 'w') as outfile:
        for row in cur.fetchall():
            json.dump(row, outfile)
            outfile.write('\n')

    outfile.close()
    print('JSON file saved')


def import_json_to_bigquery():
    bigquery_client = bigquery.Client(bq['project'])
    dataset = bigquery_client.dataset(bq['dataset'])
    table = dataset.table(bq['table'])

    # Reload table to get schema
    table.reload()

    with open(json_file['name'], 'rb') as source_file:
        job = table.upload_from_file(
            source_file, source_format='NEWLINE_DELIMITED_JSON'
        )

    job.result() # Wait job to complete
    print('Loaded {} rows into {}:{}.'.format(job.output_rows, bq['dataset'], bq['table']))


if __name__ == '__main__':
    export_postgres_to_json()
    import_json_to_bigquery()
    os.remove(json_file['name]'])
