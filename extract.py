import influxdb_client

bucket = "adsb"
url="http://127.0.0.1:8086"

client = influxdb_client.InfluxDBClient(
    url=url
)

query_api = client.query_api()
query = 'from(bucket:"adsb")\
|> range(start: -1m)'
result = query_api.query(query=query)
results = []
for table in result:
    for record in table.records:
        results.append((record.get_field(), record.get_value()))

print(results)