import apache_beam as beam
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Settings
with open('../config.json') as json_conf:
    conf = json.load(json_conf)
    project = conf['project']
    postgres = conf['postgres']
    bq = conf['bigquery']
    json_file = conf['file']

def export_data_to_pcolls(pipeline):
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (postgres['dbname'],
                                                                       postgres['user'],
                                                                       postgres['host'],
                                                                       postgres['pass']))
    cur = conn.cursor(name='serverSideCursor', cursor_factory=RealDictCursor)
    cur.itersize = postgres['cur_itersize']
    query = "" + postgres['query'] + ""
    cur.execute(query=query)

    pcolls = []
    batchIter = 1
    batchSize = 10000
    while True:
        batch = cur.fetchmany(batchSize)
        if not batch:
            break

        # Creata pCollections for every batch
        pcoll = pipeline | "Read Batch#" + str(batchIter) >> beam.Create(batch)
        pcolls.append(pcoll)
        batchIter += 1

    conn.close()
    return pcolls


def run():
    argv = [
        '--project={}'.format(project['name']),
        '--job-name={}'.format(project['dataflow']),
        '--setup_file={}'.format(project['setup']),
        '--save_main_session',
        '--staging_location=gs://{}/staging'.format(project['bucket']),
        '--temp_location=gs://{}/staging'.format(project['bucket']),
        '--runner=DataflowRunner'
    ]

    p = beam.Pipeline(argv=argv)
    batchCollection = export_data_to_pcolls(p)
    merged = tuple(batchCollection) | "To Flatten pCollection" >> beam.Flatten()
    merged | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        '%s:%s.%s' % (bq['project'], bq['dataset'], bq['table']),
        schema=bq['schema'],
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    p.run()


if __name__ == '__name__':
    run()