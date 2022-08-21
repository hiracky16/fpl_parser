import pandas as pd
from google.cloud import storage
import os, datetime, json
from functions import read_gcs_object

# const
RAW_FILE_BUCKET = os.environ['RAW_BUCKET']
DATE_FORMAT = '%Y-%m-%d'
MAX_EVENT = 38
START_DATE = '2022-06-13'

client = storage.Client()
bucket = client.get_bucket(RAW_FILE_BUCKET)

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

start = datetime.datetime.strptime(START_DATE, DATE_FORMAT).date()
end = datetime.date.today()
dt = start
while dt <= end:
    parse_event_elements(dt)
    dt = dt + datetime.timedelta(days=1)
