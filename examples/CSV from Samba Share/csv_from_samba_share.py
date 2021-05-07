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

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "RVK2"
PATH_TO_CSVS = "/RVK2/Rohdaten"
YEARS = ["2016", "2017", "2018", "2019", "2020"]

RE_GATEWAY_ID = r"RVK_GW_([0-9]{3})_rvkc-(.*)"
RE_CSV = r"(\d{4}-\d{2}-\d{2})_rvk_daily_measurements_(.*).csv"
INDEX = "Time UTC"

# ignore the file from mac os that created in the background
IGNORE_FILE = set([".DS_Store"])

# the columns that should imported
COLLUMNS = set(
    [
        INDEX,
        "AI_V_flow_HS",
        "AI_V_flow_KES",
        "AI_V_flow_KWK",
        "AI_V_flow_TWE",
        "H_store_normiert",
        "P_L1_H1_KWK",
        "P_ges_H1_AV",
        "P_ges_H1_DH",
        "P_ref_104_ret",
        "Q_L1_H1_KWK",
        "Q_ges_H1_AV",
        "Q_ges_H1_DH",
        "S_L1_H1_KWK",
        "S_ges_H1_AV",
        "S_ges_H1_DH",
        "St_Anlage",
        "St_FP_KWK",
        "St_P_ref_active_ret",
        "St_RVK_Betr",
        "St_bereit_KWK",
        "St_betr_DH",
        "St_betr_KES",
        "St_betr_KWK",
        "St_betr_SYS",
        "U_rms_L1_AV",
        "U_rms_L2_AV",
        "U_rms_L3_AV",
        "U_rms_N_AV",
        "f_AV",
        "SET_DO_1",
        "SET_ECO_State_Out",
        "ECO_State_Out",
        "t_V_KWK",
        "t_R_KWK",
        "V_flow_KWK",
        "Q_th_KWK",
        "t_V_HS",
        "t_R_HS",
        "V_flow_HS",
        "Q_th_HS",
        "t_V_Sp",
        "t_V_KES",
        "t_R_KES",
        "V_flow_KES",
        "Q_th_KES",
        "t_V_TWE",
        "t_R_TWE",
        "V_flow_TWE",
        "Q_th_TWE",
        "P_IST_System",
        "P_el_KWK",
        "P_z_ges_AV",
        "P_z_ges_DH",
        "P_z_ges_KWK",
        "Q_BG_KES",
        "Q_BG_KWK",
        "Q_BG_ges",
        "W_z_ges_AV_bez",
        "W_z_ges_AV_einsp",
        "W_z_ges_DH_bez",
        "W_z_ges_KWK_bez",
        "W_z_ges_KWK_einsp",
        "t_A",
        "t_V_Sp",
        "t_Sp_h_000",
        "t_Sp_h_010",
        "t_Sp_h_020",
        "t_Sp_h_030",
        "t_Sp_h_040",
        "t_Sp_h_050",
        "t_Sp_h_060",
        "t_Sp_h_070",
        "t_Sp_h_080",
        "t_Sp_h_090",
        "t_Sp_h_100",
        "t_Sp_h_110",
        "t_Sp_h_120",
        "t_Sp_h_130",
        "t_Sp_h_140",
        "t_Sp_h_150",
        "t_Sp_m",
        "t_Sp_o",
        "t_Sp_u",
    ]
)

# this function parse the dates in the csv
def DATE_PARSER(date_string: str):
    if type(date_string) is float:
        return float("NaN")

    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")


username = os.getenv("SAMBA_USERNAME")
password = os.getenv("SAMBA_PASSWORD")
host = os.getenv("SAMBA_HOST")
share = os.getenv("SAMBA_SHARE")

smb_fs = fs.smbfs.SMBFS(
    host,
    username=username,
    passwd=password,
    timeout=15,
    port=445,
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
