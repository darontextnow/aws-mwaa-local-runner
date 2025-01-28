"""Module contains common constants used in this de_airflow project and in running MWAA code."""
from de_utils.constants import ENV_IS_PROD, ENV_VALUE
from os import sep, path
from kubernetes.client.models import V1EnvVar, V1ResourceRequirements

MWAA_S3_DAGS_DIR = f"s3://textnow-mwaa-{ENV_VALUE}/de-airflow/dags/"
ECR_TOOLING_ROOT_URL = "695141026374.dkr.ecr.us-east-1.amazonaws.com"

# for DAG run_date parameter which allows templated run_date or default of 1 day lag
DAG_RUN_DATE_1_DAY_LAG = "{{ (dag_run.conf or {}).get('run_date') or ds }}"

# for DAG run_date parameter which allows templated run_date or default of 2 day lag
DAG_RUN_DATE_2_DAY_LAG = "{{ (dag_run.conf or {}).get('run_date') or macros.ds_add(ds, -1) }}"

DAG_DEFAULT_CATCHUP = True if ENV_IS_PROD else False  # only catchup when in prod. Otherwise, only run latest date.

AIRFLOW_STAGING_KEY_PFX = "airflow_staging_data"
FUNCTIONAL_TEST_DATA_S3_DIR = "s3://tn-snowflake-dev/de_functional_test_data_files/"

DAGS_DIR = path.abspath(path.realpath(__file__) + "/../../") + sep
DAGS_DEPENDENCIES_DIR = path.join(DAGS_DIR, "dag_dependencies") + sep
SQL_SCRIPTS_DIR = path.join(DAGS_DEPENDENCIES_DIR, "sql-scripts")

FRAUD_DISABLER_IMAGE = f"{ECR_TOOLING_ROOT_URL}/ds/fraud_disabler:1.5.0"
FRAUD_DISABLER_ARGUMENTS = ["/usr/local/bin/disablemanual"]
FRAUD_DISABLER_BASE_KUBERNETES_ENV = [
    V1EnvVar("TN_REQUEST_SERVICE_NAME", "ds_disable_job"),
    V1EnvVar("TN_DISABLE_ADDR", ("disable.prod.us-east-1.textnow.io:443" if ENV_IS_PROD
                                 else "disable.stage.us-east-1.textnow.io:443")),
    V1EnvVar("TN_FILE_FORMAT", "username"),
]
FRAUD_DISABLER_CONTAINER_RESOURCES = V1ResourceRequirements(requests={"cpu": "100m", "memory": "200Mi"},
                                                            limits={"cpu": "250m", "memory": "512Mi"})

DBT_RUN_DEPENDENT_SCHEDULE = "30 13 * * *"  # run at 1:30pm UTC every day
DBT_PROJECT_DIR = path.join(DAGS_DEPENDENCIES_DIR, "dbt-snowflake") + sep
