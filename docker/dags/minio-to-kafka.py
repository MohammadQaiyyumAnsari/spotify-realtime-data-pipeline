import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from botocore.client import Config
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
#               LOAD ENVIRONMENT VARIABLES
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ----- MinIO Configuration -----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "spotify")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "bronze/")

# ----- Snowflake Configuration -----
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "RAW_SPOTIFY_EVENTS")

# ----- Local File Path -----
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")

# ------------------------------------------------------
#               PYTHON TASK FUNCTIONS
# ------------------------------------------------------

def extract_from_minio():
    """
    Extract all .json event files from MinIO -> combine -> save locally.
    """
    # Direct valid credentials and S3v4 signature configuration
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadminpassword",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    response = s3.list_objects_v2(Bucket="spotify", Prefix="bronze/")
    contents = response.get("Contents", [])

    all_events = []
    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        data = s3.get_object(Bucket="spotify", Key=key)
        lines = data["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            if not line.strip():
                continue
            try:
                all_events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)

    print(f"✅ Extracted {len(all_events)} events from MinIO and saved to {LOCAL_TEMP_PATH}")
    return LOCAL_TEMP_PATH


def load_raw_to_snowflake(**context):
    """
    Load raw data directly into Snowflake Bronze table via batch insert.
    """
    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    if not file_path or not os.path.exists(file_path):
        print("⚠️ No raw data file found.")
        return

    with open(file_path, "r") as f:
        events = json.load(f)

    if not events:
        print("⚠️ No events found to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        event_id STRING,
        user_id STRING,
        song_id STRING,
        artist_name STRING,
        song_name STRING,
        event_type STRING,
        device_type STRING,
        country STRING,
        timestamp STRING
    );
    """
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(create_table_sql)

    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (
            event_id, user_id, song_id, artist_name, song_name,
            event_type, device_type, country, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            e.get("event_id"),
            e.get("user_id"),
            e.get("song_id"),
            e.get("artist_name"),
            e.get("song_name"),
            e.get("event_type"),
            e.get("device_type"),
            e.get("country"),
            e.get("timestamp")
        )
        for e in events
    ]

    cur.executemany(insert_sql, records)
    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Loaded {len(records)} raw records into Snowflake table: {SNOWFLAKE_TABLE}")


# ------------------------------------------------------
#               AIRFLOW DAG DEFINITION
# ------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_minio_to_snowflake_bronze",
    default_args=default_args,
    description="Load raw Spotify events from MinIO to Snowflake Bronze table",
    schedule="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_minio
    )

    load_task = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=load_raw_to_snowflake
    )

    extract_task >> load_task