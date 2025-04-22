import os


def setup_gcp_auth(path_to_credentials="config/dbt-user-creds.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_to_credentials
    print(f"GCP credentials loaded from {path_to_credentials}")
