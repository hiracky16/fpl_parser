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

def parse_event_fixtures(dt: datetime.date):
    for event in range(1, MAX_EVENT+1):
        path = f'api=fpl_api/type=fixtures/date={dt.strftime(DATE_FORMAT)}/event={event}/data.json'
        print(path)
        obj = read_gcs_object(path)

        if not obj or len(obj) == 0:
            continue

        fixtures = []
        for o in obj:
            del o['stats']
            fixtures.append(o)

        df = pd.json_normalize(obj)
        output_path = f'gs://{RAW_FILE_BUCKET}/api=fpl_api/type=parsed_fixtures/date={dt.strftime(DATE_FORMAT)}/event={event}/data.csv'
        print(output_path)
        df.to_csv(output_path, index=False)

dt = datetime.datetime.now(JST).date()
if len(sys.argv) > 1:
    dt = datetime.datetime.strptime(sys.argv[1], DATE_FORMAT).date()

print(dt)
parse_event_fixtures(dt)
