import influxdb_client

bucket = "adsb"
url="http://127.0.0.1:8086"

client = influxdb_client.InfluxDBClient(
    url=url
)

query_api = client.query_api()

#query last record for given flight/hex combitation in last 10 minutes
query = 'from(bucket: "adsb") \
  |> range(start: -10m) \
  |> filter(fn: (r) => r._measurement == "adsb_icao") \
  |> group(columns: ["flight","hex"])\
  |> last()'

result = query_api.query(query=query)
results = []
for table in result:
    print(table)
    for record in table.records:
        print(record.values)