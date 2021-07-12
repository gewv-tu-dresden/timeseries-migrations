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
    target_measurements,
    field_mapping,
    measurements_with_extern_temperature,
)

load_dotenv()

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "EXPERIMANTAL"
SOURCE_DB = os.getenv("SOURCE_DB")

INFLUXDB_V1_HOST = os.getenv("INFLUXDB_V1_HOST")
INFLUXDB_V1_PATH = os.getenv("INFLUXDB_V1_PATH")
INFLUXDB_V1_PORT = os.getenv("INFLUXDB_V1_PORT")

MAX_NUM_ENTRIES = 100000


# this function parse the dates in the csv
def DATE_PARSER(date_string: str):
    if type(date_string) is float:
        return float("NaN")

    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")


influx_v1_client = InfluxDBClient(
    INFLUXDB_V1_HOST,
    path=INFLUXDB_V1_PATH,
    port=INFLUXDB_V1_PORT,
    database=SOURCE_DB,
    ssl=True,
    verify_ssl=True,
)
influx_v2_client = TimeseriesClient.from_env_properties()

# **********************************************************
#   main function
# **********************************************************


def main():
    old_relevant_fields = []
    new_relevant_fields = []

    for key, value in field_mapping.items():
        if value is not None:
            old_relevant_fields.append(key)

            if value not in new_relevant_fields:
                new_relevant_fields.append(value)

    for measurement in target_measurements:
        measurement_package = {}

        for field in old_relevant_fields:
            field_index = new_relevant_fields.index(field_mapping[field])
            res = influx_v1_client.query(
                f"select value, dev_eui from {field} where device_name = '{measurement}'"
            )

            results = list(res)
            if len(results) < 1:
                continue

            for entry in results[0]:
                di = dateutil.parser.isoparse(entry["time"])
                devEUI = entry["dev_eui"]

                if di not in measurement_package:
                    measurement_package[di] = [devEUI] + [NaN] * len(
                        new_relevant_fields
                    )

                if measurement_package[di][0] != devEUI:
                    raise Exception(
                        "We have different dev_euis for the same entry, ohhh no!!!"
                    )

                measurement_package[di][field_index + 1] = entry["value"]

        columns = list.copy(new_relevant_fields)
        if measurement in measurements_with_extern_temperature:
            columns[
                list.index(columns, "object_temperature1")
            ] = "object_temperatureExtern"
            columns[
                list.index(columns, "object_temperature2")
            ] = "object_temperatureIntern"

        df = pd.DataFrame.from_dict(
            measurement_package,
            orient="index",
            columns=["devEUI"] + columns,
        )

        for i in range(0, len(df), MAX_NUM_ENTRIES):
            start = i
            end = min(i + MAX_NUM_ENTRIES, len(df))
            df_slice = df[start:end]

            influx_v2_client.write_a_dataframe(
                project=TARGET_BUCKET,
                measurement_name=measurement,
                dataframe=df_slice,
                tag_columns=["devEUI"],
            )

            sleep(1)

        print(f"Migrate the points of device {measurement}")

    print("Complete the migration!")


if __name__ == "__main__":
    main()
