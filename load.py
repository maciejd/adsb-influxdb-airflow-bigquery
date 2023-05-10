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
        write_disposition='WRITE_APPEND'
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
    #print(df.to_string())
    load_to_bq(df)
