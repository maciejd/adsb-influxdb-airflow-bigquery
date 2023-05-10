from google.cloud import bigquery

import extract
import transform


def load_to_bq(dataframe):
    client = bigquery.Client()

    table_id = "adsb-de.adsb.messages5m"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("hex", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("flight", bigquery.enums.SqlTypeNames.STRING),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        # write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


if __name__ == "__main__":
    df = extract.query_last_5m()
    df = transform.merge_dataframes(df)
    load_to_bq(df)
