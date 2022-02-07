#!/usr/bin/python3
# coding: utf-8

import click
import requests
import json
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from collections import defaultdict
import pandas as pd

INFLUX_HOST = "dashdot.local"
INFLUX_PORT = 8086

@click.command()
def extract_fronius_archive():
    start = (datetime.today() - timedelta(days=1))
    end = datetime.today()
    url = 'http://10.0.0.85/solar_api/v1/GetArchiveData.cgi?Scope=Device&DeviceClass=Inverter&DeviceId=1&StartDate={}&EndDate={}&Channel=Current_DC_String_1&Channel=Current_DC_String_2&Channel=Voltage_DC_String_1&Channel=Voltage_DC_String_2'.format(start.strftime("%Y-%m-%d"), end)
    response = requests.get(url)
    print("[{}] Fronius archive data request code: {}".format(datetime.now(), response))
    results = json.loads(response.text)

    results_subset = results["Body"]["Data"]["inverter/1"]["Data"]
    channels = ["Current_DC_String_1", "Current_DC_String_2", "Voltage_DC_String_1", "Voltage_DC_String_2"]
    # Collapse the dict to rescale by timestamp
    times_dict = defaultdict(dict)
    for channel in channels:
        for time, value in results_subset[channel]["Values"].items():
            times_dict[int(time)][channel] = float(value)

    data = [{
        "measurement": "archive_DC",
        "time": pd.to_datetime(str(start + timedelta(seconds=t))).tz_localize("UTC").strftime("%Y-%m-%dT%H:%m:%S"),
        "fields": v,
        } for t,v in times_dict.items()]

    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT)
    client.switch_database("grafana")
    influx_result = client.write_points(data)
    print(
        "[{}] Fronius Archive Influx write {}".format(
            datetime.now(), "Succeeded" if influx_result else "Failed"
        )
    )


if __name__ == "__main__":
    exit(extract_fronius_archive())

