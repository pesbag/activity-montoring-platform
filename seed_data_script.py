import datetime

import pandas as pd
from pathlib import Path
import mysql.connector
counter =0
db = mysql.connector.connect(
        host="db",
        user="root",
        password="secret",
        database="stationDb"
    )
def insert_values_to_sql_tables(row,counter):
    counter +=1
    cursor = db.cursor()
    sql = "INSERT IGNORE INTO station_info (Id,Name,Sector,Status,CreatedAt) VALUES (%s,%s,%s,%s,%s)"
    val = row.strip().split(",")
    val.append(datetime.datetime.now())
    cursor.execute(sql, val)
    db.commit()
    print(f"row {counter} saved successfully!")

BASE_DIR = Path(__file__).resolve().parent
file_path = BASE_DIR / "Data" / "stations.csv"
with open(file_path, "r") as f:
    f.readline(1) # skipp the header/titles oh the file
    for line in f:
        insert_values_to_sql_tables(line,counter)
        print(line)


