from flask import Flask, request
app = Flask(__name__) 

import sqlite3
import requests
from tqdm import tqdm


import json 
import numpy as np
import pandas as pd

@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips/')
def get_all_trips():
        conn = make_connection()
        trips = get_all_trips(conn)
        return trips.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/<id>')
def route_trips_id(id):
    conn = make_connection()
    trip = get_trip_id(id, conn)
    return trip.to_json()

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/total_duration') 
def get_average_duration():
        conn = make_connection()
        total_duration = get_total_duration(conn)
        return total_duration.to_json()

@app.route('/trips/average_duration/<id>')
def route_averagebikeid_duration(id):
    conn = make_connection()
    averagebikeid_duration = get_averagebikeid_duration(id, conn)
    return averagebikeid_duration.to_json()

@app.route('/trips/bike_data/<specified_bikeid>')
def route_get_bike_data(specified_bikeid):
    conn = make_connection()
    bike_data = get_bike_data(specified_bikeid, conn)
    return bike_data

    ########### Function #######
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(id, conn):
    query = f"""SELECT * FROM trips WHERE id = {id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f'''INSERT INTO trips values {data}'''
    try:
            conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_total_duration(conn):
    query = f'''SELECT SUM(duration_minutes) ,subscriber_type FROM trips GROUP BY subscriber_type'''
    result = pd.read_sql_query(query,conn)
    return result 

def get_averagebikeid_duration(id, conn):
    query = f"""SELECT AVG(duration_minutes) , id FROM trips WHERE id = {id} GROUP BY id"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_bike_data(specified_bikeid,conn):
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE bikeid LIKE ({specified_bikeid})"
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('subscriber_type').agg({
    'start_station_id' : 'count',
    'end_station_id' : 'count',
    'duration_minutes' : 'mean'
})
    return result.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)


