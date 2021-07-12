import os
from dotenv import load_dotenv
import dateutil.parser
from influxdb import InfluxDBClient
from numpy import NaN
import pandas as pd
from gewv_timeseries_client import TimeseriesClient
from datetime import datetime
from time import sleep
from examples.from_influx_v1.mappings import (
    sides,
    time_ranges,
)

load_dotenv()

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "EXPERIMANTAL"
SOURCE_DB = os.getenv("SOURCE_DB")

INFLUXDB_V1_HOST = os.getenv("INFLUXDB_V1_HOST")
INFLUXDB_V1_PATH = os.getenv("INFLUXDB_V1_PATH")
INFLUXDB_V1_PORT = os.getenv("INFLUXDB_V1_PORT")
INFLUXDB_V1_USERNAME = os.getenv("INFLUXDB_V1_USERNAME")
INFLUXDB_V1_PASSWORD = os.getenv("INFLUXDB_V1_PASSWORD")

MAX_NUM_ENTRIES = 100000

influx_v1_client = InfluxDBClient(
    INFLUXDB_V1_HOST,
    path=INFLUXDB_V1_PATH,
    port=INFLUXDB_V1_PORT,
    username=INFLUXDB_V1_USERNAME,
    password=INFLUXDB_V1_PASSWORD,
    database=SOURCE_DB,
    ssl=True,
    verify_ssl=True,
)
influx_v2_client = TimeseriesClient.from_env_properties()

# **********************************************************
#   main function
# **********************************************************


def main():
    for side_name, measurements in sides.items():
        influx_v1_client.switch_database(side_name)

        for measurement_name, fields in measurements.items():
            fields_joined = '"' + '","'.join(fields) + '"'

            if measurement_name == "OUTDOOR_TEMPERATURE":
                fields_joined = fields_joined + ',"devEUI"'

            start = time_ranges[side_name]["start"]
            end = time_ranges[side_name]["end"]

            pr = pd.period_range(start=start, end=end)

            for period in pr:
                res = influx_v1_client.query(
                    f'select {fields_joined} from "{side_name}"."autogen"."{measurement_name}" where time > \'{period.start_time.isoformat()}Z\' and time < \'{period.end_time.isoformat()}Z\''
                )

                results = list(res)
                if len(results) < 1:
                    continue

                # convert the timestamps to datetime objects
                for entry in results[0]:
                    entry["time"] = dateutil.parser.isoparse(entry["time"])

                df = pd.DataFrame.from_records(data=results[0], index="time")
                tag_columns = []
                if measurement_name == "OUTDOOR_TEMPERATURE":
                    tag_columns.append("devEUI")

                for i in range(0, len(df), MAX_NUM_ENTRIES):
                    start = i
                    end = min(i + MAX_NUM_ENTRIES, len(df))
                    df_slice = df[start:end]

                    influx_v2_client.write_a_dataframe(
                        project=TARGET_BUCKET,
                        measurement_name=measurement_name,
                        dataframe=df_slice,
                        tag_columns=tag_columns,
                        additional_tags={"database": side_name},
                    )

                    sleep(1)

                print(
                    f"Migrate the points from {period.start_time} to {period.end_time} of device {measurement_name}"
                )
            print(f"Migrate the points of device {measurement_name}")

    # print("Complete the migration!")


if __name__ == "__main__":
    main()
