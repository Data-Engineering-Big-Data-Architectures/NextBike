from google.cloud import bigquery
import os
import json
import requests
import time
from google.cloud.exceptions import Conflict

# set google application credentials to access the GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sonorous-antler-389221-f61095f6af9a.json'

# BigQuery configuration
project_id = 'sonorous-antler-389221'
dataset_id = 'nextbike'
table_id = 'Live'

# Instantiates a BigQuery client
bigquery_client = bigquery.Client(project=project_id)

def create_table_if_not_exists():
    """Create a BigQuery table if it doesn't exist."""
    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    table_ref = dataset.table(table_id)
    table = bigquery.Table(table_ref)
    schema = [
        bigquery.SchemaField('vendor_latitude', 'FLOAT'),
        bigquery.SchemaField('vendor_longitude', 'FLOAT'),
        bigquery.SchemaField('vendor_name', 'STRING'),
        bigquery.SchemaField('currency', 'STRING'),
        bigquery.SchemaField('country_code', 'STRING'),
        bigquery.SchemaField('country_name', 'STRING'),
        bigquery.SchemaField('booked_bikes', 'INTEGER'),
        bigquery.SchemaField('set_point_bikes', 'INTEGER'),
        bigquery.SchemaField('available_bikes', 'INTEGER'),
        bigquery.SchemaField('vat', 'STRING'),
        bigquery.SchemaField('city_uid', 'INTEGER'),
        bigquery.SchemaField('city_latitude', 'FLOAT'),
        bigquery.SchemaField('city_longitude', 'FLOAT'),
        bigquery.SchemaField('vendor_alias', 'STRING'),
        bigquery.SchemaField('city_name', 'STRING'),
        bigquery.SchemaField('num_places', 'INTEGER'),
        bigquery.SchemaField('refresh_rate', 'STRING'),
        bigquery.SchemaField('city_booked_bikes', 'INTEGER'),
        bigquery.SchemaField('city_set_point_bikes', 'INTEGER'),
        bigquery.SchemaField('city_available_bikes', 'INTEGER'),
        bigquery.SchemaField('return_to_official_only', 'BOOLEAN'),
        bigquery.SchemaField('station_uid', 'INTEGER'),
        bigquery.SchemaField('station_latitude', 'FLOAT'),
        bigquery.SchemaField('station_longitude', 'FLOAT'),
        bigquery.SchemaField('station_name', 'STRING'),
        bigquery.SchemaField('station_number', 'INTEGER'),
        bigquery.SchemaField('station_booked_bikes', 'INTEGER'),
        bigquery.SchemaField('station_bikes', 'INTEGER'),
        bigquery.SchemaField('bikes_available_to_rent', 'INTEGER'),
        bigquery.SchemaField('bike_racks', 'INTEGER'),
        bigquery.SchemaField('free_racks', 'INTEGER'),
        bigquery.SchemaField('special_racks', 'INTEGER'),
        bigquery.SchemaField('free_special_racks', 'INTEGER'),
        bigquery.SchemaField('bike_number', 'STRING'),
        bigquery.SchemaField('bike_type', 'INTEGER'),
        bigquery.SchemaField('active', 'BOOLEAN'),
        bigquery.SchemaField('state', 'STRING'),
        bigquery.SchemaField('electric_lock', 'BOOLEAN'),
        bigquery.SchemaField('boardcomputer', 'INTEGER')
    ]
    table.schema = schema

    try:
        table = bigquery_client.create_table(table)  # API request
        print(f"Created table {table.table_id}")
        time.sleep(3)
    except Conflict:
        print(f"Table {table.table_id} already exists.")

# URL of NextBike API
url = 'https://api.nextbike.net/maps/nextbike-live.json'


def insert_into_big_query():
    # Make a GET request to the NextBike API
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        print("Data fetched from API")
        data = response.json()
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)
        table = bigquery_client.get_table(table_ref)
        count = 0
        rows = []

        for country in data['countries']:
            for city in country['cities']:
                for station in city['places']:
                    for bike  in station['bike_list']:
                        message = {
                            'vendor_latitude' : country.get('lat'),
                            'vendor_longitude' : country.get('lng'),
                            'vendor_name' : country.get('name'),
                            'currency' : country.get('currency'),
                            'country_code' : country.get('country'),
                            'country_name' : country.get('country_name'),
                            'booked_bikes' : country.get('booked_bikes'),
                            'set_point_bikes' : country.get('set_point_bikes'),
                            'available_bikes' : country.get('available_bikes'),
                            'vat' : country.get('vat'),
                            'city_uid' : city.get('uid'),
                            'city_latitude' : city.get('lat'),
                            'city_longitude' : city.get('lng'),
                            'vendor_alias' : city.get('alias'),
                            'city_name' : city.get('name'),
                            'num_places' : city.get('num_places'),
                            'refresh_rate' : city.get('refresh_rate'),
                            'city_booked_bikes' : city.get('booked_bikes'),
                            'city_set_point_bikes' : city.get('set_point_bikes'),
                            'city_available_bikes' : city.get('available_bikes'),
                            'return_to_official_only' : city.get('return_to_official_only'),
                            'station_uid' : station.get('uid'),
                            'station_latitude' : station.get('lat'),
                            'station_longitude' : station.get('lng'),
                            'station_name' : station.get('name'),
                            'station_number' : station.get('number'),
                            'station_booked_bikes' : station.get('booked_bikes'),
                            'station_bikes' : station.get('bikes'),
                            'bikes_available_to_rent' : station.get('bikes_available_to_rent'),
                            'bike_racks' : station.get('bike_racks'),
                            'free_racks' : station.get('free_racks'),
                            'special_racks' : station.get('special_racks'),
                            'free_special_racks' : station.get('free_special_racks'),
                            'bike_number' : bike.get('number'),
                            'bike_type' : bike.get('bike_type'),
                            'active' : bike.get('active'),
                            'state' : bike.get('state'),
                            'electric_lock' : bike.get('electric_lock'),
                            'boardcomputer' : bike.get('boardcomputer')
                        }
                    
                        row = [
                            message.get('vendor_latitude'),
                            message.get('vendor_longitude'),
                            message.get('vendor_name'),
                            message.get('currency'),
                            message.get('country_code'),
                            message.get('country_name'),
                            message.get('booked_bikes'),
                            message.get('set_point_bikes'),
                            message.get('available_bikes'),
                            message.get('vat'),
                            message.get('city_uid'),
                            message.get('city_latitude'),
                            message.get('city_longitude'),
                            message.get('vendor_alias'),
                            message.get('city_name'),
                            message.get('num_places'),
                            message.get('refresh_rate'),
                            message.get('city_booked_bikes'),
                            message.get('city_set_point_bikes'),
                            message.get('city_available_bikes'),
                            message.get('return_to_official_only'),
                            message.get('station_uid'),
                            message.get('station_latitude'),
                            message.get('station_longitude'),
                            message.get('station_name'),
                            message.get('station_number'),
                            message.get('station_booked_bikes'),
                            message.get('station_bikes'),
                            message.get('bikes_available_to_rent'),
                            message.get('bike_racks'),
                            message.get('free_racks'),
                            message.get('special_racks'),
                            message.get('free_special_racks'),
                            message.get('bike_number'),
                            message.get('bike_type'),
                            message.get('active'),
                            message.get('state'),
                            message.get('electric_lock'),
                            message.get('boardcomputer')
                        ]
                        rows.append(row)
                        count = count + 1
                        if(count % 5000 == 0):
                            if(insertRows(bigquery_client, table, rows, count)):
                                rows = []
        insertRows(bigquery_client, table, rows, count)
    else:
        print(f'Request failed with status code {response.status_code}')
        
def insertRows(bigquery_client, table, rows, count):
    errors = bigquery_client.insert_rows(table, rows)
    if errors:
        print(f"Encountered errors while inserting row: {errors}")
        return False
    else:
        print("Inserted Records", count)
        return True

# Create the BigQuery table if it doesn't exist
create_table_if_not_exists()
insert_into_big_query()
