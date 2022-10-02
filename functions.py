from google.cloud import storage
import os, datetime, json

# const
RAW_FILE_BUCKET = os.environ['RAW_BUCKET']

client = storage.Client()
bucket = client.get_bucket(RAW_FILE_BUCKET)

def read_gcs_object(path):
    try:
        blob = bucket.get_blob(path)
        text = blob.download_as_string().decode('utf-8')
        obj = json.loads(text)
        return obj
    except Exception as e:
        print(e)
        return None

