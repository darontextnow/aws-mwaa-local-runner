# About aws-mwaa-local-runner

This (forked) repository provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally.

Due to some complexity in local runner setup, Daron recommend's we mainly use this local runner for testing out package versions, configuration changes, and environment upgrades. A simpler local environment (described in de-airflow README) can be used for local dag dev testing.

## About the CLI

The CLI builds a Docker container image locally that’s similar to a MWAA production image. This allows you to run a local Apache Airflow environment to develop and test DAGs, custom plugins, and dependencies before deploying to MWAA.

## Prerequisites

- **macOS**: [Install Docker Desktop](https://docs.docker.com/desktop/).
- **Linux/Ubuntu**: [Install Docker Compose](https://docs.docker.com/compose/install/) and [Install Docker Engine](https://docs.docker.com/engine/install/).
- **Windows**: Windows Subsystem for Linux (WSL) to run the bash based command `mwaa-local-env`. Please follow [Windows Subsystem for Linux Installation (WSL)](https://docs.docker.com/docker-for-windows/wsl/) and [Using Docker in WSL 2](https://code.visualstudio.com/blogs/2020/03/02/docker-in-wsl2), to get started.

```bash
git clone git@github.com:darontextnow/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
```

This repo is setup to authenticate to AWS using the withokta profile name of "dev-datascience" and should use the DataScience role of the dev AWS account. 
If you use a different profile name, you must change the value for the environment variable "AWS_PROFILE" in the file ''aws-mwaa-local-runner/docker/docker-compose-local.yml'' to the name of the profile you authenticate locally with.

## Get started

### Step one: Building the Docker image:

```bash
./mwaa-local-env build-image
```

### Step two: Deploy our requirements and DAGs from the de-airflow repo:

```bash
python ../deploy_scripts/deploy_latest_to_local_mwaa_runner.py
```

### Step three: Running Apache Airflow

Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```bash
./mwaa-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

### Step four: Accessing the Airflow UI

- Url: http://localhost:8080/
- Username: `admin`
- Password: `test`


## Adding and Testng Changes to requirements.txt.

1. If airflow lcoal runner is currently running, stop it (Ctrl+C).
2. Update de-airflow repo's requirements.txt with changes to be tested.
2. Run the de-airflow script to deploy your changes:  `python ../deploy_scripts/deploy_latest_to_local_mwaa_runner.py`

You can test the changes out by either starting the local runner again, `./mwaa-local-env start` or by running `./mwaa-local-env test-requirements`.
Then, carefully analyze the console output for any errors that arise during the pip installation process or other issues.

## Testing Changes to DAGs

Simply use the de-airflow deploy script `python ../deploy_scripts/deploy_latest_to_local_mwaa_runner.py` to push your changes to the local runner and wait for the DagBag to pick those changes up from the local runner dir.

