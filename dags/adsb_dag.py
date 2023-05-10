from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from google.cloud import bigquery

from datetime import datetime, timedelta
import pandas as pd

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
        schedule=timedelta(minutes=5),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["adsb"],
) as dag:
    def query_to_df():
        hook = InfluxDBHook(conn_id="influx_db_conn")
        sql = 'from(bucket: "adsb") |> range(start: -5m) \
                |> filter(fn: (r) => r._measurement == "adsb_icao") \
                |> group(columns: ["flight","hex"]) \
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        return hook.query_to_df(sql)

    def transform_data_from_influx(**kwargs):
        ti = kwargs['ti']
        influx_data = ti.xcom_pull(task_ids='query_data')
        # concatenate multiple dataframes, group by airframe hex pick last NaN value for each
        #todo handle if single dataframe
        df = pd.concat(influx_data)
        df = df.groupby(['hex']).last().reset_index()
        # fields loookup https://github.com/wiedehopf/readsb/blob/dev/README-json.md#aircraftjson
        # ignore error as 'calc_track' is not always returned
        df = df.drop(columns=['result', 'table', 'calc_track'], errors='ignore')
        print(df.to_string())
        return df

    def load_to_bq(**kwargs):
        ti = kwargs['ti']
        dataframe = ti.xcom_pull(task_ids='process_data')
        client = bigquery.Client()

        table_id = "adsb-de.adsb.messages5m"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("hex", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("flight", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition='WRITE_APPEND'
        )

        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()
        print("BQ job finished")

        # table = client.get_table(table_id)
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    #need to use PythonOperator to use query_to_df() method, InfluxDBOperator uses query()
    query_data = PythonOperator(
        task_id='query_data',
        python_callable=query_to_df,
        provide_context=True,
        do_xcom_push=True,
        dag=dag
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=transform_data_from_influx,
        provide_context=True,
        do_xcom_push=True,
        dag=dag
    )

    load_data_to_bigquery = PythonOperator(
        task_id = 'load_data_to_bigquery',
        python_callable=load_to_bq,
        provide_context = True,
        dag = dag
    )

    query_data >> process_data >> load_data_to_bigquery
