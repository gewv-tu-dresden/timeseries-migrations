from fs.sshfs import SSHFS
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

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

with my_fs.open(
    f"{share}/station12/MUC500_ID_6891d08038b1_TS_1621219543.csv", "r"
) as csv_file:
    data = pd.read_csv(
        csv_file,
        delimiter=";",
    )

    print(data)
