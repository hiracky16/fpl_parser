import pandas as pd
from google.cloud import storage
import os, datetime, json, sys
from functions import read_gcs_object
from functions import read_gcs_object
from dateutil import tz
JST = tz.gettz('Asia/Tokyo')

# const
RAW_FILE_BUCKET = os.environ['RAW_BUCKET']
DATE_FORMAT = '%Y-%m-%d'
MAX_EVENT = 38

def parse_bootstrap_events(dt: datetime.date):
    stats = []
    path = f'api=fpl_api/type=bootstrap-static/date={dt.strftime(DATE_FORMAT)}/data.json'
    print(path)
    obj = read_gcs_object(path)

    if not obj or 'events' not in obj or  len(obj['events']) == 0:
        return
    df = pd.json_normalize(obj['events'])
    output_path = f'gs://{RAW_FILE_BUCKET}/api=fpl_api/type=events/date={dt.strftime(DATE_FORMAT)}/data.csv'
    print(output_path)
    df.to_csv(output_path, index=False)

dt = datetime.datetime.now(JST).date()
if len(sys.argv) > 1:
    dt = datetime.datetime.strptime(sys.argv[1], DATE_FORMAT).date()

print(dt)
parse_bootstrap_events(dt)
