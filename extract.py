import influxdb_client


bucket = "adsb"
url = "http://127.0.0.1:8086"


def query_last_5m():
    client = influxdb_client.InfluxDBClient(
        url=url
    )

    query_api = client.query_api()

    # query last record for given flight/hex combitation in last 10 minutes
    query = 'from(bucket: "adsb") \
      |> range(start: -5m) \
      |> filter(fn: (r) => r._measurement == "adsb_icao") \
      |> group(columns: ["flight","hex"]) \
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'

    list_of_df = query_api.query_data_frame(query=query)
    return list_of_df
