from fs.sshfs import SSHFS
import pandas as pd
from dotenv import load_dotenv
import os
import re
from gewv_timeseries_client import TimeseriesClient
from datetime import datetime
from pytz import UTC
from time import sleep

load_dotenv()

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "EXPERIMENTAL"

RE_STATION_ID = r"station\d{10}"
RE_CSV = r"MUC500_ID_\w{12}_TS_\d{10}.csv"
INDEX = "Timestamp"

# ignore the file from mac os that created in the background
IGNORE_FILE = set([".DS_Store"])

DESCRIPTION_MAPPING = {
    'Actuality duration': 'actuality_duration'
}

# the columns that should imported
COLLUMNS = set(
    [
        INDEX,
        "DeviceId",
        "Value0",
        "Unit0",
        'Scale0',
        "Description0",
    ]
)
        
# this function parse the dates in the csv
def DATE_PARSER(date_string: str):
    return datetime.fromtimestamp(int(date_string), tz=UTC)

username = os.getenv("SFTP_USERNAME")
password = os.getenv("SFTP_PASSWORD")
host = os.getenv("SFTP_HOST")
share = os.getenv("SFTP_SHARE")


my_fs = SSHFS(
    host=host,
    user=username,
    passwd=password,
    pkey=None,
    timeout=10,
    port=22,
    keepalive=10,
    compress=False,
    config_path="~/",
)

client = TimeseriesClient.from_env_properties()
client.create_bucket(TARGET_BUCKET)

# **********************************************************
#   migration script
# **********************************************************

def migrate_csv(path: str, station_name: str, csv_name: str):
    with my_fs.open(path, "r") as csv_file:
        try:
            data = pd.read_csv(
                csv_file,
                delimiter=";",
                index_col=INDEX,
                parse_dates=True,
                date_parser=DATE_PARSER,
                usecols=lambda c: c in COLLUMNS,
            )
        except ValueError:
            print(f"Failed to parse the file {path}. Skip this csv.")
            return
        except KeyError:
            print(f"Failed to parse the file {path}. Skip this csv.")
            return

        logfile = open(f"D:\\timeseries-migrations\\examples\\CSV from SFTP Share\\station12_in_InfluxDB.txt",'r+')
        text = logfile.read().splitlines()
        if csv_name in text:
            print(f"Upload  of {csv_name} is already done. Skip this csv.")
            return

        # drop negative timestamps
        #data = data[data.index.year >= int(year)]

        for column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
            data[column] = data[column].astype(float)

        if len(data) < 1 or len(data.columns) < 1:
            print("Receive empty csv. Skip that day.")
            return

        _measurment = DESCRIPTION_MAPPING[data['Description22'][0]]
        data['value'] = data['Value22'] * data['Scale22']
        data['meterID'] = data['DeviceId']
        del data['Scale22']
        del data['Description22']
        del data['Unit22']
        del data['Value22']
        del data['DeviceId']

        print(data)
        client.write_a_dataframe(
            project=TARGET_BUCKET,
            measurement_name=_measurment,
            dataframe=data,
            tag_columns=['meterID'],
            additional_tags={'station': station_name}
        )

        print(f"Finish migration. Write {len(data)} points to db.")


        if len(data) < 1 or len(data.columns) < 1:
            print("Receive empty csv. Skip that day.")
            return

        logfile.write(f"\n{csv_name}") 
        logfile.close()
        print(f"Finish migration. Write {len(data)} points to db.")


# **********************************************************
#   main function
# **********************************************************

station_folders = my_fs.listdir(f"{share}")

for station in station_folders:
    if station in IGNORE_FILE:
        continue
    match = list(re.finditer(RE_STATION_ID, station))[0]
    if match:
        sta_name = match.group(1)
        sta_id = match.group(2)
        path_to_csvs = f"{share}/{sta_name}/{RE_CSV}"

        csvs = my_fs.listdir(path_to_csvs)

        for csv in csvs:
            matches = list(re.finditer(RE_CSV, csv)) 
            csv_name = csv           
            if len(matches) < 1:
                continue
            if matches[0]:
                day = matches[0].group(1)
                #if day_exists(year, gw_number, day):
                    #continue
                print(
                    f"Start to migrate CSV from station {sta_name} from the {day}."
                )
                try:
                    migrate_csv(path_to_csvs, sta_name, csv_name)
                except Exception:
                    # the migration failed first time. Wait a minute and try again ...
                    sleep(0.1)
                    migrate_csv(path_to_csvs, sta_name, csv_name)
        print(f"Measurment migration for the station {sta_name} completed.")
print(f"Measurment migration for all stations completed.")

