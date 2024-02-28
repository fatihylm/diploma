import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Verbindungsdaten für InfluxDB
token = "ToponLtJ9rh5TzCLnufO5refQXz6wpoy7YIAFkktISdfxxK08lDNBgg1gB1Uw6UMYDaH6Cwy1Yt2P-6B22m5zw=="
org = "htld"
url = "http://localhost:8086"
bucket = "monitoring"


# Funktion zum Formatieren des Zeitstempels
def format_timestamp(timestamp):
    timestamp = timestamp.replace('"', '')  # Entfernen von Anführungszeichen
    # Anpassen des Formats entsprechend dem Format Ihrer Zeitstempel
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z')
    return int(dt.timestamp() * 1e9)  # Nanosekunden für InfluxDB


# Erstellen Sie eine Instanz des InfluxDB-Clients
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)


def read_specific_columns(file_path, selected_columns):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        data_list = [line.strip().split(',') for line in lines]

    max_columns = max(len(row) for row in data_list)
    data_list_uniform = [row + [None] * (max_columns - len(row)) for row in data_list]
    data = pd.DataFrame(data_list_uniform[1:], columns=data_list_uniform[0])

    if selected_columns:
        data = data[selected_columns]

    # Schreiben der Daten in InfluxDB
    for i, row in data.iterrows():
        # Zeitstempel formatieren
        try:
            formatted_timestamp = format_timestamp(row['timestamp'])
        except Exception as e:
            print(f"Fehler beim Formatieren des Zeitstempels: {e}")
            continue

        point = (
          Point("elektrolyser_test")  # Der Messungsname
          .field("h2_flow", float(row['h2_flow']) if row['h2_flow'] else 0)  # Felder hinzufügen
          .field("h2_total", float(row['h2_total']) if row['h2_total'] else 0)
          .time(formatted_timestamp, WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)


file_path = 'electrolyser_el21_8F6FFB0B32DD8BC0E8D3826F105E58533703F5DC_6237.csv'
selected_columns = ['timestamp', 'h2_flow', 'h2_total']

data_cleaned = read_specific_columns(file_path, selected_columns)

client.close()
