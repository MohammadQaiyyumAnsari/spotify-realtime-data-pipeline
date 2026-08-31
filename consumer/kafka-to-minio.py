import json
import os
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# ---------- Configuration ----------
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "spotify")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9002")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadminpassword"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
# ---------- Confluent Kafka Consumer Setup ----------
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'group.id': 'spotify-minio-group-v4',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "127.0.0.1:9092")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5))

# ---------- MinIO Connection ----------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket '{MINIO_BUCKET}' already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket '{MINIO_BUCKET}'.")

# ---------- Confluent Kafka Consumer Setup ----------
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'group.id': 'spotify-minio-group-fresh',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

print(f"🎧 Listening for events on Kafka topic '{KAFKA_TOPIC}'...")

batch = []

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process message
        event_str = msg.value().decode('utf-8')
        event = json.loads(event_str)
        song = event.get('song_name') or event.get('track_name', 'Unknown Song')
        artist = event.get('artist_name', 'Unknown Artist')
        print(f"📩 Received event: {song} by {artist}")
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            now = datetime.now(timezone.utc)
            date_path = now.strftime("date=%Y-%m-%d/hour=%H")
            file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
            file_path = f"bronze/{date_path}/{file_name}"

            json_data = "\n".join([json.dumps(e) for e in batch])

            s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=file_path,
                Body=json_data.encode("utf-8")
            )

            print(f"✅ Uploaded {len(batch)} events to MinIO: {file_path}")
            batch = []

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()