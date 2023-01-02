import pandas as pd
from google.cloud import storage
import os, datetime, json, sys
from functions import read_gcs_object
from dateutil import tz
JST = tz.gettz('Asia/Tokyo')

# const
RAW_FILE_BUCKET = os.environ['RAW_BUCKET']
DATE_FORMAT = '%Y-%m-%d'
MAX_EVENT = 38

def parse_event_elements(dt: datetime.date):
    for event in range(1, MAX_EVENT+1):
        stats = []
        path = f'api=fpl_api/type=event-live/date={dt.strftime(DATE_FORMAT)}/event={event}/data.json'
        print(path)
        obj = read_gcs_object(path)

        if not obj or 'elements' not in obj or len(obj['elements']) == 0:
            continue

        for o in obj['elements']:
            stat = o['stats']
            stat.update({'id': o['id']})
            stats.append(stat)
        df = pd.json_normalize(stats)
        output_path = f'gs://{RAW_FILE_BUCKET}/api=fpl_api/type=parsed_event-live_20221114/date={dt.strftime(DATE_FORMAT)}/event={event}/data.csv'
        print(output_path)
        df.to_csv(output_path, index=False)

dt = datetime.datetime.now(JST).date()
if len(sys.argv) > 1:
    dt = datetime.datetime.strptime(sys.argv[1], DATE_FORMAT).date()

print(dt)
parse_event_elements(dt)
