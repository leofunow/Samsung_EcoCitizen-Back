from datetime import datetime
from fastapi import FastAPI
import psycopg2
import json
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/sensor_data")
async def sensor_data():
    conn = psycopg2.connect(
        host="rc1d-gh60uaznwt0p6gc1.mdb.yandexcloud.net",
        database="telemetry",
        user="guest",
        password="BO1JXIZ&toSpG25A",
        port=6432
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM telemetry_hist")
        data = cur.fetchall()
        data = [
            {
                "telemetry_timestamp": i[0],
                "device_timestamp": i[1],
                "device_name": i[2],
                "sensor_model": i[3],
                "location": {
                    "latitude": json.loads(i[4])[0]["latitude"],
                    "longitude": json.loads(i[4])[1]["longitude"]
                },
                "sensordatavalues": json.loads(i[5])
            }
            for i in data
        ]
        return data
    except Exception as e:
        print(e)
        return {"error": "Failed to fetch data"}
    

@app.get("/sensor_table")
async def sensor_table():
    conn = psycopg2.connect(
        host="rc1d-gh60uaznwt0p6gc1.mdb.yandexcloud.net",
        database="telemetry",
        user="guest",
        password="BO1JXIZ&toSpG25A",
        port=6432
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM telemetry_hist")
        devices = []
        data = cur.fetchall()
        data = [
            {
                "telemetry_timestamp": i[0],
                "device_timestamp": i[1],
                "device_name": i[2],
                "sensor_model": i[3],
                "location": {
                    "latitude": json.loads(i[4])[0]["latitude"],
                    "longitude": json.loads(i[4])[1]["longitude"]
                },
                "sensordatavalues": json.loads(i[5])
            }
            for i in data
        ]
        for i in data:
            found = False
            for j in devices:
                if i["device_name"] == j["device_name"]:
                    found = True
                    # 2024-04-22T17:43:17.903743
                    # if datetime.strptime(i["telemetry_timestamp"], "%Y-%m-%d %H:%M:%S.%f") > datetime.strptime(j["telemetry_timestamp"], "%Y-%m-%d %H:%M:%S.%f"):
                    if i["telemetry_timestamp"] > j["telemetry_timestamp"]:
                        j["telemetry_timestamp"] = i["telemetry_timestamp"]
                        j["sensordatavalues"] = i["sensordatavalues"]
                        j["location"] = i["location"]
            if not found:
                devices.append(i)
        return devices
    except Exception as e:
        print(e)
        return {"error": "Failed to fetch data"}