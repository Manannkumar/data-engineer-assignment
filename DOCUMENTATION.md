# SQS Data Pipeline — Documentation

## Requirements

- Python 3.8+
- Docker and Docker Compose
- Go
- Make

Install Python dependencies:
```bash
pip install boto3 sqlalchemy psycopg2-binary
```

---

## Environment Setup

The project uses a `.env` file for AWS credentials. LocalStack accepts any dummy values:

```
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-1
```

The Makefile loads this automatically so credentials never need to be set manually.

---

## How to Run

Run the full pipeline with one command:

```bash
make all
```

This runs everything in order, starts Docker, pushes test messages, runs the ETL, and shows the results.

To run steps individually:

```bash
make up        # start Docker containers (LocalStack + PostgreSQL)
make messages  # push test messages to the SQS queue
make run       # run the ETL pipeline
make show      # print all records from the database
make down      # stop Docker containers
```

---

## How It Works

**Extract**
Connects to LocalStack SQS using boto3 and reads messages in batches until the queue is empty.

**Transform**
Two message formats are handled:
- `route` format : has a list of legs with `from`, `to`, `duration`, and `started_at` (string `DD/MM/YYYY HH:MM:SS`). Departure is the first leg's origin, destination is the last leg's destination, end time is calculated by adding duration to the last leg's start time.
- `locations` format : has a list of entries with `location` and `timestamp` (Unix epoch). Departure is the first location, destination is the last.

Malformed messages (invalid JSON) are skipped and logged.

**Load**
Each record is upserted into PostgreSQL. If a record with the same `id` already exists, it gets overwritten. This handles duplicate messages without creating duplicate rows.

**Delete**
A message is deleted from SQS only after it is successfully saved to the database. If the insert fails, the message stays in the queue and gets retried automatically.

---

## Language Choice

Python. It has straightforward libraries for everything needed here boto3 for SQS, SQLAlchemy for the database, and psycopg2 for PostgreSQL. The code is easy to read and modify.

---

## Database

PostgreSQL running in Docker. It is defined in `docker-compose.yml` alongside LocalStack so the entire stack starts with one command.

Table schema:
```sql
CREATE TABLE trips (
    id          INTEGER PRIMARY KEY,
    mail        TEXT,
    name        TEXT,
    departure   TEXT,
    destination TEXT,
    start_date  TIMESTAMP,
    end_date    TIMESTAMP
)
```

PostgreSQL was chosen over SQLite because it is production-grade, supports upsert natively, and runs cleanly in Docker.

---

## Challenges

**LocalStack version** : the latest image requires a paid license. Fixed by pinning to version 3.8 in docker-compose.yml.

**Two different date formats** : route messages use a human-readable string, location messages use Unix epoch. Both are parsed into Python datetime objects before storing.

**Duplicate messages** : two messages in the queue had the same `id`. Handled with upsert so only one row exists per id.

---

## Known Limitations

**No Dead Letter Queue (DLQ)** : if a message keeps failing on every retry, it will block that message indefinitely. In production, a DLQ should be configured so that after a set number of failures SQS automatically moves the message out of the main queue. The pipeline can then continue and the failed message is preserved for debugging.

**No timezone handling** : dates are stored as TIMESTAMP without timezone info. In production, UTC should be enforced explicitly.

**Hardcoded AWS credentials in etl.py** : fine for LocalStack but in production these should come from environment variables or AWS IAM roles, never hardcoded.

**print() instead of proper logging** : works for this use case but in production a logging framework with log levels (INFO, ERROR) would make monitoring much easier.
