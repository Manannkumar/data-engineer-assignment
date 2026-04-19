import boto3
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

QUEUE_URL = "http://sqs.ap-south-1.localhost.localstack.cloud:4566/000000000000/test-queue"

sqs = boto3.client(
    "sqs",
    region_name="ap-south-1",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

engine = create_engine("postgresql://etl_user:etl_pass@localhost:5432/etl_db")


def setup_db():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trips (
                id INTEGER PRIMARY KEY,
                mail TEXT,
                name TEXT,
                departure TEXT,
                destination TEXT,
                start_date TIMESTAMP,
                end_date TIMESTAMP
            )
        """))
        conn.commit()
    print("DB ready")


def transform(msg):
    body = msg["Body"]

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        print(f"Skipping malformed message: {body}")
        return None

    result = {
        "id": data["id"],
        "mail": data["mail"],
        "name": f"{data['name']} {data['surname']}",
    }

    if "route" in data:
        legs = data["route"]
        first_leg = legs[0]
        last_leg = legs[-1]
        start_dt = datetime.strptime(first_leg["started_at"], "%d/%m/%Y %H:%M:%S")
        end_dt = datetime.strptime(last_leg["started_at"], "%d/%m/%Y %H:%M:%S") + timedelta(minutes=last_leg["duration"])
        result["trip"] = {
            "departure": first_leg["from"],
            "destination": last_leg["to"],
            "start_date": start_dt,
            "end_date": end_dt
        }

    elif "locations" in data:
        locs = data["locations"]
        start_dt = datetime.utcfromtimestamp(locs[0]["timestamp"])
        end_dt = datetime.utcfromtimestamp(locs[-1]["timestamp"])
        result["trip"] = {
            "departure": locs[0]["location"],
            "destination": locs[-1]["location"],
            "start_date": start_dt,
            "end_date": end_dt
        }

    return result


def load(record, conn):
    conn.execute(text("""
        INSERT INTO trips (id, mail, name, departure, destination, start_date, end_date)
        VALUES (:id, :mail, :name, :departure, :destination, :start_date, :end_date)
        ON CONFLICT (id) DO UPDATE SET
            mail = EXCLUDED.mail,
            name = EXCLUDED.name,
            departure = EXCLUDED.departure,
            destination = EXCLUDED.destination,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date
    """), {
        "id": record["id"],
        "mail": record["mail"],
        "name": record["name"],
        "departure": record["trip"]["departure"],
        "destination": record["trip"]["destination"],
        "start_date": record["trip"]["start_date"],
        "end_date": record["trip"]["end_date"]
    })
    conn.commit()


def run():
    setup_db()

    processed = 0
    skipped = 0

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1
        )
        batch = response.get("Messages", [])
        if not batch:
            break

        for msg in batch:
            record = transform(msg)

            if record is None:
                skipped += 1
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                continue

            try:
                with engine.connect() as conn:
                    load(record, conn)
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                print(f"id={record['id']} done")
                processed += 1
            except Exception as e:
                print(f"id={record['id']} failed: {e}")

    print(f"\nprocessed={processed}, skipped={skipped}")


if __name__ == "__main__":
    run()