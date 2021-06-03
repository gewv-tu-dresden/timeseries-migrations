# Load CSV from sftp-Server

Small script that connects to a sftp-server and loads there CSV to send the rows in the file to the timeseries db. The script create a small database to store the process.

## Required Enviroment Variables

You can easy use a .env-file to store the variables.

```
SFTP_USERNAME - username for sftp-server
SFTP_PASSWORD - password for sftp-server
SFTP_HOST - address of the sftp-server
SAMBA_SHARE - share on the server
TARGET-BUCKET - the bucket on the Timeseries DB to store data
INFLUXDB_V2_URL - the url to connect to Timeseries DB
INFLUXDB_V2_ORG - default destination organization for writes and queries
INFLUXDB_V2_TOKEN - the token to use for the authorization
INFLUXDB_V2_TIMEOUT - socket timeout in ms (default value is 10000)
INFLUXDB_V2_VERIFY_SSL - set this to false to skip verifying SSL certificate when calling API from https server (default true)
```
