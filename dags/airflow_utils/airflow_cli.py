"""This script allows us to run some airflow CLI commands from local env that apply to MWAA running environments.
To run a CLI command locally:
    First use withokta to authenticate to the AWS account you intend to run the command in.
    Run in your terminal the command:
        python <path to this file>.py -e <MWAA env to apply command to> -c <the command to run>
    Example: python dags/airflow_utils/airflow_cli.py -e de-mwaa-prod -c "variables list"
"""

import boto3
import requests
import base64
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--mwaa_env", required=True,
                    help=("Enter the name of the MWAA env to apply commands to. "
                          "Current values include de-mwaa-prod and de-mwaa-dev"))
parser.add_argument("-c", "--command", required=True,
                    help=("The command and command args to run. No need to specify airflow. "
                          "Supported commands can be found here:  "
                          "https://docs.aws.amazon.com/mwaa/latest/userguide/"
                          "airflow-cli-command-reference.html#airflow-cli-commands"))
args = parser.parse_args()

client = boto3.client('mwaa', region_name="us-east-1")

mwaa_cli_token = client.create_cli_token(Name=args.mwaa_env)

mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])

mwaa_response = requests.post(
    mwaa_webserver_hostname,
    headers={
        'Authorization': mwaa_auth_token,
        'Content-Type': 'text/plain'
    },
    data=args.command
)

mwaa_std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')

print(mwaa_response.status_code)
print(mwaa_std_err_message)
print(mwaa_std_out_message)
