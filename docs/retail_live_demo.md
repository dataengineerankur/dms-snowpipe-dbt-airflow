# Retail Live Demo

## Stack
- Streamlit UI on `http://localhost:8502`
- FastAPI on `http://localhost:8001/docs`
- Airflow on `http://localhost:8080`
- Redpanda Console on `http://localhost:8081`
- Spark Structured Streaming writes raw JSON events to S3

## Data Paths
- Raw: `s3://dms-snowpipe-dev-05d6e64a/retail-live/raw/transactions/`
- Bronze: `s3://dms-snowpipe-dev-05d6e64a/retail-live/bronze/transactions/`

## AWS prerequisites
- Configure AWS CLI locally before starting the stack.
- Either export `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and optional `AWS_SESSION_TOKEN`) or set `AWS_PROFILE` to a profile that can access the retail bucket.
- Confirm access with `aws sts get-caller-identity` and `aws s3 ls s3://dms-snowpipe-dev-05d6e64a/`.

## Start
```bash
./scripts/start_retail_stack.sh
```

## Stop
```bash
./scripts/stop_retail_stack.sh
```

## Flow
1. Submit a transaction in the Streamlit UI.
2. FastAPI publishes the order event to Kafka topic `retail.transactions.raw`.
3. Spark Structured Streaming consumes from Kafka and writes raw JSON into the S3 raw prefix.
4. Airflow DAG `retail_raw_to_bronze` promotes raw S3 events into bronze parquet files every 2 minutes.
5. The UI monitor page shows Kafka estimates, Spark status, recent transactions, and raw/bronze object counts.
