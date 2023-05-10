import datetime as dt
from airflow import DAG
import airflow.operators
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Operators; we need this to operate!

with DAG(
    "adsb_etl",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        # "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Load 5min ADSB aggregates to BQ",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["adsb"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    query_influxdb_task = InfluxDBOperator(
        influxdb_conn_id="influx_db_conn",
        task_id="query_influxdb",
        sql='from(bucket: "adsb") \
        |> range(start: -5m) \
        |> filter(fn: (r) => r._measurement == "adsb_icao") \
        |> group(columns: ["flight","hex"]) \
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")',
        dag=dag,
    )

    # transform_dataframe = PythonOperator(
    #     task_id="sleep",
    #     depends_on_past=False,
    #     bash_command="sleep 5",
    #     retries=3,
    # )

#     load_to_bigquery = BigQueryUpsertTableOperator(
#         task_id="upsert_table",
#         dataset_id="adsb",
#         table_resource={
#             "tableReference": {"tableId": "messages5m"},
#             "expirationTime": (int(time.time()) + 300) * 1000,
# })

    query_influxdb_task




