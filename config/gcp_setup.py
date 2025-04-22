from google.cloud import storage, bigquery
from config.env_config import PROJECT_ID


def create_gcs_bucket(bucket_name):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name, location="US")
        print(f" Created GCS bucket: {bucket_name}")
    else:
        print(f" GCS bucket already exists: {bucket_name}")


def create_bigquery_dataset(dataset_name):
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_name}")
    try:
        client.get_dataset(dataset_ref)
        print(f" BigQuery dataset already exists: {dataset_name}")
    except Exception:
        dataset = client.create_dataset(dataset_ref)
        print(f" Created BigQuery dataset: {dataset.dataset_id}")
