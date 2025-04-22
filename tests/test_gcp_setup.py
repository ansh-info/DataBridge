import os
import sys

# Ensure project root is in sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
from config.gcp_setup import create_gcs_bucket, create_bigquery_dataset

# Step 1: Load GCP credentials
setup_gcp_auth()

# Step 2: Create GCS bucket
create_gcs_bucket(GCS_BUCKET)

# Step 3: Create BigQuery dataset
create_bigquery_dataset(DATASET_NAME)
