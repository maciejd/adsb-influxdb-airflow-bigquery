# adsb-influxdb-airflow-bigquery

This repo loads ADSB data to InfluxDB and demonstrates the use of Airflow by querying the data form InfluxDB, transforming it and loading it to BigQuery.

## Prerequisites
Access to ADSB data exposed _somewhere_. I personally use Raspberry Pi with [wiedehopf/readsb](https://github.com/wiedehopf/readsb)

## Required configuration

### GCP
- Create project and dataset in BigQuery
- Create Service account in GCP with the following roles:
  - `BigQuery Data Editor`
  - `BigQuery Job User`
- download service account key and store it in project root directory as `service_account_key.json` (or update namepath in `docker-compose.yml` respectively)

### docker-compose.yml
- modify value `ADSBHOST` to IP of your ADSB host / Raspberry Pi

### adsb_dag.py
- Update table_id to your BigQuery `<project>.<dataset>.<expected_table_name>`
- The job will create a table if it doesn't exist

### Airflow (after running the containers)
- Log in and create a new connection in Airflow
  - Connection id: `influx_db_conn`
  - Connection type: `influxdb`
  - Schema: `http`
  - Host: `influxdb18`
  - Port: `8086`

## Run
- 'docker-compose up -d'
- Airflow will be available on port 8080 with default credentials of `airflow/airflow`
