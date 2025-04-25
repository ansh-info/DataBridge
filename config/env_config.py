import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = os.getenv("DATASET_NAME")
GCS_BUCKET = os.getenv("GCS_BUCKET")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")  # for static pipeline
KAGGLE_KEY = os.getenv("KAGGLE_KEY")          # for static pipeline
