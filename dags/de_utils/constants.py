"""Contains constants shared by multiple apps"""
from de_utils.utils import get_env
from de_utils.enums import Env
import os

# AWS accounts
PROD_ACCOUNT_ID = 981322793757
DEV_ACCOUNT_ID = 193830186226
TOOLING_ACCOUNT_ID = 695141026374
ENFLICK_ACCOUNT_ID = 696281505104

# Constants specific to the environment code is currently running in
ENV_ENUM = get_env()  # Returns Env.PROD or Env.DEV
ENV_VALUE = ENV_ENUM.value  # Returns "prod" or "dev"
ENV_IS_DEV = ENV_ENUM == Env.DEV
ENV_IS_PROD = ENV_ENUM == Env.PROD
ENV_IS_LOCAL = False if os.getenv("AIRFLOW__CUSTOM__ENV") else True

# Constants for commonly used Airflow connection ids
SF_CONN_ID = f"snowflake_{ENV_VALUE}_etl_write"
DBT_SNOWFLAKE_CONN_ID = f"snowflake_dbt_{ENV_VALUE}"
ADJUST_CONN_ID = "adjust_api"
AWS_CONN_ID_K8 = "k8s_ds"
AWS_CONN_ID_GROWTH = "aws_growth"
GCP_CONN_ID_GROWTH = "gcp_growth"
MYSQL_CONN_ID_CENTRAL = "tn_central"
MYSQL_CONN_ID_INVENTORY = "mysql_inventory_production"
MYSQL_CONN_ID_PHONE = "phone_number_repository"
MYSQL_CONN_ID_PORTAGE = "portage"
MYSQL_CONN_ID_SPRINT_AMS = "sprint_ams"
MYSQL_SHARDS_CONN_IDS = {"tndb_prod_1": "mysql_tndb_prod_1",
                         "tndb_prod_2": "mysql_tndb_prod_2",
                         "tndb_prod_3": "mysql_tndb_prod_3",
                         "tndb_prod_4": "mysql_tndb_prod_4"}
MYSQL_CONN_ID_ARCHIVE = "mysql_archive"

# Constants for commonly used S3 buckets
SF_BUCKET = f"tn-snowflake-{ENV_VALUE}"
UPLOADS_BUCKET = f"textnow-uploads-{ENV_VALUE}"
DATA_SCIENCE_BUCKET = f"tn-data-science-{ENV_VALUE}"
DATA_SCIENCE_MODELS_BUCKET = "tn-data-science-prod"  # can retrieve models in this bucket from dev/local envs
DATA_LAKE_BUCKET_LEGACY = "tn-data-lake"
DATA_LAKE_BUCKET = f"tn-data-lake-{ENV_VALUE}"

# Constants for commonly used staging locations in Snowflake
STAGE_SF = f"{ENV_VALUE}.public.S3_TN_SNOWFLAKE_{ENV_VALUE}"
STAGE_DATA_LAKE_LEGACY = "prod.public.S3_TN_DATA_LAKE_LEGACY"  # works for sketchy_reg_logs, there is no dev source.

# Constants for misc commonly used strings
ADJUST_APP_TOKEN_TN_IOS_FREE = "szvvsxe5b4t2"
ADJUST_APP_TOKEN_TN_ANDROID = "fnz52pd8uj2r"
ADJUST_APP_TOKEN_2L_ANDROID = "vyn1230pux34"
DS_VOLUME = "data-science"

# Snowflake Warehouses
SF_WH_4XLARGE = "PROD_WH_4XLARGE" if ENV_IS_PROD else "DEV_WH_MEDIUM"
SF_WH_LARGE = "PROD_WH_LARGE" if ENV_IS_PROD else "DEV_WH_MEDIUM"
SF_WH_MEDIUM = "PROD_WH_MEDIUM" if ENV_IS_PROD else "DEV_WH_MEDIUM"
SF_WH_SMALL = "PROD_WH_SMALL" if ENV_IS_PROD else "DEV_WH_SMALL"
SF_WH_XSMALL = "PROD_WH_XSMALL" if ENV_IS_PROD else "DEV_WH_XSMALL"

# Misc Services
MLFLOW_URI = f"https://mflow.{ENV_VALUE}.us-east-1.textnow.io/"
