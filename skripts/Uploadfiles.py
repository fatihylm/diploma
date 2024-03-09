import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Verbindungsdaten für InfluxDB
token = "ToponLtJ9rh5TzCLnufO5refQXz6wpoy7YIAFkktISdfxxK08lDNBgg1gB1Uw6UMYDaH6Cwy1Yt2P-6B22m5zw=="
org = "htld"
url = "http://localhost:8086"
networkURL = "http://10.115.1.216:8086"
bucket = "Tests"

#Formatierung des Zeitstempels
def format_timestamp(timestamp):
    timestamp = timestamp.replace('"', '')  # Entfernen von Anführungszeichen
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z')
        return int(dt.timestamp() * 1e9)  # Nanosekunden für InfluxDB
    except ValueError:
        # Fallback für andere Zeitformate oder fehlerhafte Daten
        return 0


def read_specific_columns(file_path, selected_columns=None):
    try:
        # Verwendung von pandas read_csv für bessere Handhabung von CSV-Formaten
        data = pd.read_csv(file_path, usecols=selected_columns)
    except Exception as e:
        print(f"Fehler beim Lesen der Datei: {e}")
        return pd.DataFrame()

    return data


# Erstellen der Instanz des InfluxDB-Clients
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)


def write_data_to_influx(file_path, selected_columns=None):
    data = read_specific_columns(file_path, selected_columns)

    timestamp_column = data.columns[0]
    numeric_columns = [col for col in data.columns if col != timestamp_column]

    for index, row in data.iterrows():
        try:
            formatted_timestamp = format_timestamp(row[timestamp_column])
            if formatted_timestamp == 0:
                # Überspringen von Zeilen mit ungültigen Zeitstempeln
                continue
            point = Point("test").time(formatted_timestamp, WritePrecision.NS)
            for column in numeric_columns:
                try:
                    # Versuch, den Wert zu einem Float zu konvertieren
                    value = float(row[column]) if row[column] else 0.0  # Immer als Float
                except ValueError:
                    # Falls die Konvertierung fehlschlägt, Wert mit 0.0 ersetzen
                    value = 0.0
                point = point.field(column, value)

            write_api.write(bucket=bucket, org=org, record=point)
        except Exception as e:
            print(f"Fehler beim Verarbeiten der Zeile {index}: {e}")



file_path = 'testfiles/ucm_1FAA5977A7BD89874D373523AB44FFDDC7E28165_6394 (1).csv'  # Passen Sie diesen Pfad an Ihre CSV-Datei an
write_data_to_influx(file_path)

client.close()
