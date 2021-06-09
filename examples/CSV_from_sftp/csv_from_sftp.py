import os
import fs.smbfs
from dotenv import load_dotenv
import re
import pandas as pd
from gewv_timeseries_client import TimeseriesClient
from datetime import datetime
import sqlite3
from time import sleep

load_dotenv()

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "BBFly"
PATH_TO_CSVS = "/BBFly/Rohdaten"  ## Wie lautet der Pfad zur InfluxDB?
YEARS = ["2016", "2017", "2018", "2019", "2020"]

RE_GATEWAY_ID = r"RVK_GW_([0-9]{3})_rvkc-(.*)"
RE_CSV = r"(\d{4}-\d{2}-\d{2})_rvk_daily_measurements_(.*).csv"
INDEX = "Timestamp"  ## es gibt für jeden Value einen eigenen Zeitstempel; der unterscheidet sich von Spalte A

# ignore the file from mac os that created in the background
IGNORE_FILE = set([".DS_Store"])

# the columns that should imported
COLLUMNS = set(
    [
        INDEX,
        "Value0",
        "Scale0",
        "Unit0",
        "Description0",
        "User0",
        "Timestamp0",
        "ObisId0",
        "Value1",
        "Scale1",
        "Unit1",
        "Description1",
        "User1",
        "Timestamp1",
        "ObisId1",
        "Value2",
        "Scale2",
        "Unit2",
        "Description2",
        "User2",
        "Timestamp2",
        "ObisId2",
        "Value3",
        "Scale3",
        "Unit3",
        "Description3",
        "User3",
        "Timestamp3",
        "ObisId3",
        "Value4",
        "Scale4",
        "Unit4",
        "Description4",
        "User4",
        "Timestamp4",
        "ObisId4",
        "Value5",
        "Scale5",
        "Unit5",
        "Description5",
        "User5",
        "Timestamp5",
        "ObisId5",
        "Value6",
        "Scale6",
        "Unit6",
        "Description6",
        "User6",
        "Timestamp6",
        "ObisId6",
        "Value7",
        "Scale7",
        "Unit7",
        "Description7",
        "User7",
        "Timestamp7",
        "ObisId7",
        "Value8",
        "Scale8",
        "Unit8",
        "Description8",
        "User8",
        "Timestamp8",
        "ObisId8",
        "Value9",
        "Scale9",
        "Unit9",
        "Description9",
        "User9",
        "Timestamp9",
        "ObisId9",
        "Value10",
        "Scale10",
        "Unit10",
        "Description10",
        "User10",
        "Timestamp10",
        "ObisId10",
        "Value11",
        "Scale11",
        "Unit11",
        "Description11",
        "User11",
        "Timestamp11",
        "ObisId11",
        "Value12",
        "Scale12",
        "Unit12",
        "Description12",
        "User12",
        "Timestamp12",
        "ObisId12",
        "Value13",
        "Scale13",
        "Unit13",
        "Description13",
        "User13",
        "Timestamp13",
        "ObisId13",
        "Value14",
        "Scale14",
        "Unit14",
        "Description14",
        "User14",
        "Timestamp14",
        "ObisId14",
        "Value15",
        "Scale15",
        "Unit15",
        "Description15",
        "User15",
        "Timestamp15",
        "ObisId15",
        "Value16",
        "Scale16",
        "Unit16",
        "Description16",
        "User16",
        "Timestamp16",
        "ObisId16",
        "Value17",
        "Scale17",
        "Unit17",
        "Description17",
        "User17",
        "Timestamp17",
        "ObisId17",
        "Value18",
        "Scale18",
        "Unit18",
        "Description18",
        "User18",
        "Timestamp18",
        "ObisId18",
        "Value19",
        "Scale19",
        "Unit19",
        "Description19",
        "User19",
        "Timestamp19",
        "ObisId19",
        "Value20",
        "Scale20",
        "Unit20",
        "Description20",
        "User20",
        "Timestamp20",
        "ObisId20",
        "Value21",
        "Scale21",
        "Unit21",
        "Description21",
        "User21",
        "Timestamp21",
        "ObisId21",
        "Value22",
        "Scale22",
        "Unit22",
        "Description22",
        "User22",
        "Timestamp22",
        "ObisId22",
        "Value23",
        "Scale23",
        "Unit23",
        "Description23",
        "User23",
        "Timestamp23",
        "ObisId23",
        "Value24",
        "Scale24",
        "Unit24",
        "Description24",
        "User24",
        "Timestamp24",
        "ObisId24",
    ]
)

# this function parse the dates in the csv
def DATE_PARSER(date_string: str):
    if type(date_string) is float:
        return float("NaN")

    ## return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")
    return datetime.utcfromtimestamp(int(date_string)).strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")


username = os.getenv("SFTP_USERNAME")
password = os.getenv("SFTP_PASSWORD")
host = os.getenv("SFTP_HOST")
## share = os.getenv("SAMBA_SHARE")

smb_fs = fs.smbfs.SMBFS(
    host,
    username=username,
    passwd=password,
    timeout=15,
    ## port=445,
    port=22,               ## sftp-Port
    direct_tcp=True,
    domain="dom",
)

# connect to time series db and create bucket if necassary
client = TimeseriesClient.from_env_properties()
client.create_bucket(TARGET_BUCKET)
completed_db = sqlite3.connect("completed.db")

# **********************************************************
#   functions for the process db
# **********************************************************


def create_tables():
    completed_db.execute(
        """CREATE TABLE IF NOT EXISTS year
        (
            year        TEXT NOT NULL,
            UNIQUE(year)
        );"""
    )

    completed_db.execute(
        """CREATE TABLE IF NOT EXISTS gateway
        (
            gateway     TEXT NOT NULL,
            year        TEXT NOT NULL,
            UNIQUE(gateway, year)
        );"""
    )

    completed_db.execute(
        """CREATE TABLE IF NOT EXISTS day
        (
            day         TEXT NOT NULL,
            gateway     TEXT NOT NULL,
            year        TEXT NOT NULL,
            UNIQUE(day, gateway, year)
        );"""
    )

    completed_db.commit()


def complete_a_year(year: str):
    completed_db.execute(
        f'INSERT INTO year (year) \
      VALUES ("{year}");'
    )

    completed_db.commit()


def complete_a_gateway(year: str, gateway: str):
    completed_db.execute(
        f'INSERT INTO gateway (year, gateway) \
      VALUES ("{year}", "{gateway}");'
    )

    completed_db.commit()


def complete_a_day(year: str, gateway: str, day: str):
    completed_db.execute(
        f'INSERT INTO day (year, gateway, day) \
      VALUES ("{year}", "{gateway}", "{day}");'
    )

    completed_db.commit()


def year_exists(year: str) -> bool:
    res = completed_db.execute(
        f'SELECT EXISTS(SELECT 1 FROM year WHERE year="{year}");'
    ).fetchone()

    return res[0] == 1


def gateway_exists(year: str, gateway: str) -> bool:
    res = completed_db.execute(
        f'SELECT EXISTS(SELECT 1 FROM gateway WHERE year="{year}" \
             AND gateway="{gateway}");'
    ).fetchone()

    return res[0] == 1


def day_exists(year: str, gateway: str, day: str) -> bool:
    res = completed_db.execute(
        f'SELECT EXISTS(SELECT 1 FROM day WHERE year="{year}" \
             AND gateway="{gateway}" AND day="{day}");'
    ).fetchone()

    return res[0] == 1


# **********************************************************
#   migration script
# **********************************************************


def migrate_csv(path: str, gateway_number: str):
    with smb_fs.open(path, "r") as csv_file:
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

        # drop negative timestamps
        data = data[data.index.year >= int(year)]

        for column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
            data[column] = data[column].astype(float)

        if len(data) < 1 or len(data.columns) < 1:
            print("Receive empty csv. Skip that day.")
            return

        client.write_a_dataframe(
            project=TARGET_BUCKET,
            measurement_name=f"Gateway {gateway_number}",
            dataframe=data,
        )

        print(f"Finish migration. Write {len(data)} points to db.")


# **********************************************************
#   main function
# **********************************************************

create_tables()

for year in YEARS:
    path_to_gateways = f"{PATH_TO_CSVS}/{year}"
    gateway_folders = smb_fs.listdir(f"{PATH_TO_CSVS}/{year}")

    if year_exists(year):
        continue

    for folder in gateway_folders:
        if folder in IGNORE_FILE:
            continue

        match = list(re.finditer(RE_GATEWAY_ID, folder))[0]

        if match:
            gw_number = match.group(1)
            gw_id = match.group(2)
            path_to_csvs = f"{path_to_gateways}/{folder}/CSV"

            if gateway_exists(year, gw_number):
                continue

            csvs = smb_fs.listdir(path_to_csvs)

            for csv in csvs:
                matches = list(re.finditer(RE_CSV, csv))

                if len(matches) < 1:
                    continue

                if matches[0]:
                    day = matches[0].group(1)

                    if day_exists(year, gw_number, day):
                        continue

                    print(
                        f"Start to migrate CSV from Gateway {gw_number} from the {day}."
                    )
                    try:
                        migrate_csv(
                            path=f"{path_to_csvs}/{csv}", gateway_number=gw_number
                        )

                    except Exception:
                        # the migration failed first time. Wait a minute and try again ...
                        sleep(60)
                        migrate_csv(
                            path=f"{path_to_csvs}/{csv}", gateway_number=gw_number
                        )

                    complete_a_day(year, gw_number, day)

            print(
                f"Migrate measurments for the Gateway {gw_number} in the year {year}."
            )
            complete_a_gateway(year, gw_number)

    print(f"Completed the year {year}")
    complete_a_year(year)

print("Complete the db migration!")
