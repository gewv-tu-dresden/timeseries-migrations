from fs.sshfs import SSHFS
import pandas as pd
from dotenv import load_dotenv
import os
import re
import sys
from gewv_timeseries_client import TimeseriesClient
from datetime import datetime
from pandas.core.frame import DataFrame
from pytz import UTC
from time import sleep
from typing import List
from loguru import logger

load_dotenv()
# init debugger and configure
DIR_PATH = os.path.dirname(__file__)
print(DIR_PATH)
logger.add(f"{DIR_PATH}\\Debug.log")

#LOG_FILE = os.path.basename(__file__)
#print(LOG_FILE)
LOG_FILE = os.path.splitext(os.path.basename(__file__))[0]
print(LOG_FILE)

TARGET_BUCKET = os.getenv("TARGET_BUCKET") or "EXPERIMENTAL"

RE_STATION_ID = re.compile(r"station(\d{2})")
RE_CSV = re.compile(r"MUC500_ID_(\w{12})_TS_(\d{10}).csv")
RE_DEVICE_ID_LUG = re.compile(r"LUG-*")
RE_DEVICE_ID_EFE = re.compile(r"EFE-*")
RE_DEVICE_ID_NZR = re.compile(r"NZR-*")
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
        "Scale0",
        "Description0",
        "Value1",
        "Unit1",
        "Scale1",
        "Description1",
        "Value2",
        "Unit2",
        "Scale2",
        "Description2",
        "Value3",
        "Unit3",
        "Scale3",
        "Description3",
        "Value4",
        "Unit4",
        "Scale4",
        "Description4",
        "Value5",
        "Unit5",
        "Scale5",
        "Description5",
        "Value6",
        "Unit6",
        "Scale6",
        "Description6",
        "Value7",
        "Unit7",
        "Scale7",
        "Description7",
        "Value8",
        "Unit8",
        "Scale8",
        "Description8",
        "Value9",
        "Unit9",
        "Scale9",
        "Description9",
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
# client.create_bucket(TARGET_BUCKET) # geht nicht ??berall, da nur Admins die Berechtigung besitzen, Buckets zu erstellen

# **********************************************************
#   migration script
# **********************************************************

def get_dataframe_from_index(package: DataFrame, index: str) -> DataFrame:
    output = DataFrame(index=range(len(package)))
    output.index = package.index

    output['value'] = package[f'Value{index}'] * package[f'Scale{index}']
    output['unit'] = package[f'Unit{index}']
    output['_measurment'] = package[f'Description{index}']
    output['meterID'] = package['DeviceId']

    return output  

def prepare_lug_package(package: DataFrame) -> List[DataFrame]:
    output = []

    output.append(get_dataframe_from_index(package=package, index='2'))
    output.append(get_dataframe_from_index(package=package, index='3'))
    output.append(get_dataframe_from_index(package=package, index='4'))
    output.append(get_dataframe_from_index(package=package, index='5'))
    output.append(get_dataframe_from_index(package=package, index='6'))
    output.append(get_dataframe_from_index(package=package, index='7'))
    output.append(get_dataframe_from_index(package=package, index='8'))

    return output

def prepare_efe_package(package: DataFrame) -> List[DataFrame]:
    output = []

    output.append(get_dataframe_from_index(package=package, index='1'))
    output.append(get_dataframe_from_index(package=package, index='2'))
    output.append(get_dataframe_from_index(package=package, index='3'))
    output.append(get_dataframe_from_index(package=package, index='5'))
    output.append(get_dataframe_from_index(package=package, index='7'))
    output.append(get_dataframe_from_index(package=package, index='8'))
    output.append(get_dataframe_from_index(package=package, index='9'))

    return output

def prepare_nzr_package(package: DataFrame) -> List[DataFrame]:
    output = []

    output.append(get_dataframe_from_index(package=package, index='0'))

    return output

def write_dataframe(package: DataFrame, station_name: str):
    try:
        #for p in packages_to_save:
        for p in package:
            #print(package)
            print(p['_measurment'][0])
            _measurment = p['_measurment'][0].replace(' ', '_').lower()
            print(_measurment)
            del p['_measurment']

            client.write_a_dataframe(
                project=TARGET_BUCKET,
                measurement_name=_measurment,
                dataframe=p,
                tag_columns=['meterID'],
                additional_tags={'station': station_name}
            )
    except:
        return

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
        except OSError as err:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: OS error: {0}".format(err))
            #logfile.write(f"{str(NOW)}: OS error: {0}".format(err))
            logger.add(f"{str(NOW)}: OS error: {0}".format(err)) 
            return        
        except ValueError:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Failed to parse the file {path}. Skip this csv.")
            logfile.write(f"{str(NOW)}: Failed to parse the file {path}. Skip this csv.\n") 
            return
        except KeyError:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Failed to parse the file {path}. Skip this csv.")
            logfile.write(f"{str(NOW)}: Failed to parse the file {path}. Skip this csv.\n")
            return
        except:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Unexpected error:", sys.exc_info()[0])
            logfile.write(f"{str(NOW)}: Unexpected error:", sys.exc_info()[0])
            return

        try:
            #uploaded_files = open(f"D:\\timeseries-migrations\\examples\\CSV from SFTP Share\\station12_in_InfluxDB.txt",'r+')
            uploaded_files = open(f"{DIR_PATH}\\{station_name}_in_InfluxDB.txt",'r+', encoding='utf-8')
            text = uploaded_files.read().splitlines()
            if csv_name in text:
                NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"{str(NOW)}: Upload  of {csv_name} is already done. Skip this csv.")
                logfile.write(f"{str(NOW)}: Upload  of {csv_name} is already done. Skip this csv.\n")
                return
        except FileNotFoundError as EXCPTN:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Error while reading file {EXCPTN}")
            logfile.write(f"{str(NOW)}: Error while reading file {EXCPTN}\n")
            return

        # drop negative timestamps
        #data = data[data.index.year >= int(year)]

        # for column in data.columns:
        #     data[column] = pd.to_numeric(data[column], errors="coerce")
        #     data[column] = data[column].astype(float)

        if len(data) < 1 or len(data.columns) < 1:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Receive empty csv. Skip that day.")
            logfile.write(f"{str(NOW)}: Receive empty csv. Skip that day.\n")
            return

        packages_to_save = []

        package_nzr = data[data.DeviceId.str.startswith('NZR')]
        package_lug = data[data.DeviceId.str.startswith('LUG')]
        package_efe = data[data.DeviceId.str.startswith('EFE')]

        packages_to_save.extend(prepare_lug_package(package_lug))
        #packages_to_save.append(prepare_lug_package(package_lug))
        #print(package_lug)
        print(packages_to_save)
        write_dataframe(packages_to_save, station)
        packages_to_save = []
        packages_to_save.extend(prepare_efe_package(package_efe))
        #packages_to_save.append(prepare_efe_package(package_efe))
        print(package_efe)
        write_dataframe(packages_to_save, station)
        packages_to_save = []
        packages_to_save.extend(prepare_nzr_package(package_nzr))
        #packages_to_save.append(prepare_nzr_package(package_nzr))
        print(package_nzr)  
        write_dataframe(packages_to_save, station)  

        print(packages_to_save)

        # for p in packages_to_save:
        #     _measurment = p['_measurment'][0].replace(' ', '_').lower()
        #     del p['_measurment']

        #     client.write_a_dataframe(
        #         project=TARGET_BUCKET,
        #         measurement_name=_measurment,
        #         dataframe=p,
        #         tag_columns=['meterID'],
        #         additional_tags={'station': station_name}
        #     )
        NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{str(NOW)}: Finish migration. Write {len(data)} points to db.")
        logfile.write(f"{str(NOW)}: Finish migration. Write {len(data)} points to db.\n")


        if len(data) < 1 or len(data.columns) < 1:
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Receive empty csv. Skip that day.")
            logfile.write(f"{str(NOW)}: Receive empty csv. Skip that day.\n")
            return

        #with open('path', 'wa')
        uploaded_files.write(f"{csv_name}\n") 
        uploaded_files.close()
        #NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #print(f"{str(NOW)}: Finish migration. Write {len(data)} points to db.")


# **********************************************************
#   main function
# **********************************************************

try:
    logfile = open(f"{DIR_PATH}\\{LOG_FILE}.log",'r+', encoding='utf-8')
    #station_name = os.path.__file__    
    total_lines = sum(1 for line in logfile)
    NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{str(NOW)}: Opening logfile {logfile.name}")
    logfile.write(f"{str(NOW)}: Opening logfile {logfile.name}\n") 

    station_folders = my_fs.listdir(f"{share}")

    for station in station_folders:
        if station in IGNORE_FILE:
            continue
        match = RE_STATION_ID.match(station)
        if match is None:
            continue

        #sta_id = match.group(1)
        path_to_csvs = f"{share}/{station}"
        NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{str(NOW)}: Scanning {station} for files")
        logfile.write(f"{str(NOW)}: Scanning {station} for files\n")

        csvs = my_fs.listdir(path_to_csvs)
        NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{str(NOW)}: Sorting csvs in folder {station} started")
        logfile.write(f"{str(NOW)}: Sorting csvs in folder {station} started\n")
        csvs.sort()
        NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{str(NOW)}: Sorting csvs in folder {station} finished")
        logfile.write(f"{str(NOW)}: Sorting csvs in folder {station} finished\n")

        for csv in reversed(csvs):
            match = RE_CSV.match(csv)
            if match is None:
                continue            
            
            time_in_unix = match.group(2)
            time_in_UTC = DATE_PARSER(time_in_unix)
            #if day_exists(year, gw_number, day):
                #continue
            NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{str(NOW)}: Start to migrate CSV from {station} from {time_in_UTC} (UNIX: {time_in_unix}).")
            logfile.write(f"{str(NOW)}: Start to migrate CSV from {station} from {time_in_UTC} (UNIX: {time_in_unix}).\n")
            migrate_csv(f'{path_to_csvs}/{csv}', station, csv)
            continue

        NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{str(NOW)}: Measurment migration for the station {station} completed.")
        logfile.write(f"{str(NOW)}: Measurment migration for the station {station} completed.\n")
    print(f"{str(NOW)}: Measurment migration for all stations completed.")
    logfile.write(f"{str(NOW)}: Measurment migration for all stations completed.\n")

    logfile.close
except FileNotFoundError as EXCPTN:
    NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{str(NOW)}: Error while reading file {EXCPTN}")
    logfile.write(f"{str(NOW)}: Error while reading file {EXCPTN}\n")
    

    

