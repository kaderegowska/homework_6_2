import sqlite3
from sqlite3 import Error
import csv

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def insert_stations():
    file = open('clean_stations.csv')
    contents = csv.reader(file)
    cur = conn.cursor()
    insert_records = "INSERT INTO stations (station, latitude, longitude, elevation, name, country, state) VALUES(?, ?, ?, ?, ?, ?, ?)"
    cur.executemany(insert_records, contents)
    conn.commit()

def insert_measure():
    measures = open('clean_measure.csv')
    rows = csv.reader(measures)
    cur = conn.cursor()
    insert_measures = "INSERT INTO measure (station_name, date, precip, tobs) VALUES(?, ?, ?, ?)"
    cur.executemany(insert_measures, rows)
    conn.commit()


if __name__ == "__main__":

    db_file = "database.db"   
    conn = create_connection(db_file)

    create_stations_sql = """
    -- stations table
    CREATE TABLE IF NOT EXISTS stations (
        id INTEGER PRIMARY KEY,
        station text,
        latitude float,
        longitude float,
        elevation float,
        name text,
        country text,
        state text
    );
    """

    create_measure_sql = """
    -- measure table
    CREATE TABLE IF NOT EXISTS measure (
        id INTEGER PRIMARY KEY,
        station_name text,
        date text,
        precip float,
        tobs integer,
        FOREIGN KEY (station_name) REFERENCES stations (station)
    );
    """

    if conn is not None:
        execute_sql(conn, create_stations_sql)
        execute_sql(conn, create_measure_sql)
    

    insert_stations()
    insert_measure()


    print(conn.execute("SELECT station FROM stations LIMIT 5").fetchall())



