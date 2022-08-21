import pandas as pd
from google.cloud import storage
import os, datetime, json

# const
RAW_FILE_BUCKET = os.environ['RAW_BUCKET']
DATE_FORMAT = '%Y-%m-%d'
MAX_EVENT = 38
START_DATE = '2022-06-13'

client = storage.Client()
bucket = client.get_bucket(RAW_FILE_BUCKET)

def read_gcs_object(path):
    try:
        blob = bucket.get_blob(path)
        text = blob.download_as_string().decode('utf-8')
        obj = json.loads(text)
        return obj
    except:
        return None

def parse_event_elements(dt: datetime.date):
    for event in range(1, MAX_EVENT+1):
        stats = []
        path = f'api=fpl_api/type=event-live/date={dt.strftime(DATE_FORMAT)}/event={event}/data.json'
        obj = read_gcs_object(path)

        if not obj or 'elements' not in obj or len(obj['elements']) == 0:
            continue

        for o in obj['elements']:
            stat = o['stats']
            stat.update({'id': o['id']})
            stats.append(stat)
        df = pd.json_normalize(stats)
        output_path = f'gs://{RAW_FILE_BUCKET}/api=fpl_api/type=parsed_event-live/date={dt.strftime(DATE_FORMAT)}/event={event}/data.csv'
        df.to_csv(output_path, index=False)

def parse_bootstrap_elements(dt: datetime.date):
    stats = []
    path = f'api=fpl_api/type=bootstrap-static/date={dt.strftime(DATE_FORMAT)}/data.json'

    obj = read_gcs_object(path)

    if not obj or 'elements' not in obj or  len(obj['elements']) == 0:
        return
    df = pd.json_normalize(obj['elements'])
    output_path = f'gs://{RAW_FILE_BUCKET}/api=fpl_api/type=elements/date={dt.strftime(DATE_FORMAT)}/data.csv'
    df.to_csv(output_path, index=False)

start = datetime.datetime.strptime(START_DATE, DATE_FORMAT).date()
end = datetime.date.today()
dt = start
while dt <= end:
    parse_event_elements(dt)
    parse_bootstrap_elements(dt)
    dt = dt + datetime.timedelta(days=1)
