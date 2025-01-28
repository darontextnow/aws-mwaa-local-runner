from airflow_utils import dag
from dag_dependencies.constants import ECR_TOOLING_ROOT_URL, DAG_DEFAULT_CATCHUP, DAG_RUN_DATE_1_DAY_LAG
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.dq_checks_defs import AppReviewsClassificationDQChecks
from airflow.operators.empty import EmptyOperator
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, DATA_SCIENCE_MODELS_BUCKET, ENV_VALUE
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 31),
    schedule=None,  # Triggered by dq_checks_core_google_reviews
    default_args={
        "owner": "Satish",
        "retry_delay": timedelta(minutes=15),
        "retries": 1,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def app_reviews_classification():
    s3_pfx = "app_reviews"
    run_date = "{{ data_interval_start.date() }}"

    wait_ios_review_extract = ExternalTaskDeferredSensor(
        task_id="wait_ios_reviews_extract",
        external_dag_id="app_store_reviews_extract",
        execution_date_fn=lambda dt: dt.replace(hour=1, minute=10),
        timeout=3600 * 3  # wait for up to 3 hours
    )

    collect_android_reviews = SnowflakeToS3DeferrableOperator(
        task_id="load_input_android_reviews_to_s3",
        sql=f"""
            SELECT 
                id,
                author_name,
                text,
                last_modified,
                rating,
                client_type,
                app_version
            FROM core.google_play_reviews
            WHERE last_modified::date = '{run_date}'
                AND (language = 'en')
                AND (client_type IN ('TN_ANDROID', '2L_ANDROID'))
                AND (LOWER(text) != 'n/a') 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_modified DESC) = 1
            ORDER BY last_modified
        """,
        s3_bucket=SF_BUCKET,
        s3_key=f"{s3_pfx}/input_reviews/{run_date}/android_reviews.csv",
        file_format="(TYPE = CSV COMPRESSION = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
                    " EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE = 10000000"
    )

    collect_ios_reviews = SnowflakeToS3DeferrableOperator(
        task_id="load_input_ios_reviews_to_s3",
        sql=f"""
                SELECT 
                    id,
                    author_name,
                    text,
                    last_modified,
                    rating,
                    client_type,
                    app_version
                FROM core.appstore_reviews
                WHERE last_modified::DATE >= '{run_date}'::DATE - INTERVAL '7 DAYS'
                    AND (country_code IN ('US','CA', 'MX'))
                    AND (client_type = ('TN_IOS_FREE'))
                    AND (LOWER(text) != 'n/a') 
                    AND (id <> '11853821129') --is one particular review that causes failures in scoring process
                QUALIFY ROW_NUMBER() OVER (PARTITION BY author_name, text ORDER BY last_modified DESC) = 1
                ORDER BY last_modified
            """,
        s3_bucket=SF_BUCKET,
        s3_key=f"{s3_pfx}/input_reviews/{run_date}/ios_reviews.csv",
        file_format="(TYPE = CSV COMPRESSION = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
                    " EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE = 10000000"
    )

    input_reviews = EmptyOperator(task_id="collect_inputs")

    run_dq_checks = DQChecksOperator(
        task_id="run_app_reviews_dq_checks",
        dq_checks_instance=AppReviewsClassificationDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    for client in ['ios', 'android']:
        previous_task = input_reviews
        models = get_model_defs(s3_pfx, run_date)
        client_type = "('TN_IOS_FREE')" if client == 'ios' else "('TN_ANDROID', '2L_ANDROID')"
        delete_condition = f">= '{run_date}'::DATE - interval '7 days'" if client == 'ios' else f"= '{run_date}'::DATE"
        for model in models:
            name = f"{client}_{model.replace('_pipeline', '')}"
            current_task = KubernetesPodOperator(
                task_id=name,
                image=ECR_TOOLING_ROOT_URL + "/ds/app_reviews_classification:v1.2",
                name=name.replace('_', '-'),
                arguments=get_args_list(f"{model}.py", models[model]["args"], client),
                container_resources=models[model]["resources"]
            )
            previous_task >> current_task
            previous_task = current_task

        sf_load = S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{client}_app_reviews_snowflake",
            table="app_reviews",
            schema="analytics",
            s3_loc=f"{s3_pfx}/cost_pipeline/{run_date}/{client}_app_reviews_cost_pipeline.csv",
            file_format="(TYPE = CSV DATE_FORMAT = AUTO SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING,
            pre_exec_sql=[
                f"DELETE FROM {ENV_VALUE}.analytics.app_reviews WHERE (LAST_MODIFIED::DATE {delete_condition})\
                AND (client_type IN {client_type})"]
        )

        previous_task >> sf_load >> run_dq_checks

    wait_ios_review_extract >> [collect_android_reviews, collect_ios_reviews] >> input_reviews


def get_model_defs(s3_pfx: str, run_date):
    """Returns defs for all models to be run.
    Keeping this out of top level DAG to save from DagBag having to re-instantiate all the classes used in the defs.
    """
    models_pfx = f"s3://{DATA_SCIENCE_MODELS_BUCKET}/{s3_pfx}"
    data_pfx = f"s3://{SF_BUCKET}/{s3_pfx}"
    return {
        "text_fixing_pipeline": {
            "args": {
                "language_model_path": f"{models_pfx}/models/probabilistic_language_model/gpt2/",
                "sentence_simplification_path": f"{models_pfx}/models/sentence_simplification/bert_srl/",
                "punctuate_restoration_path": f"{models_pfx}/models/punctuation_restoration/t5/",
                "grammar_correction_path": f"{models_pfx}/models/grammar_correction/t5/",
                "input_s3_loc": f"{data_pfx}/input_reviews/{run_date}/:app_os_reviews.csv",
                "output_s3_loc": f"{data_pfx}/text_fixing_pipeline/{run_date}/:app_os_app_reviews_text_fixing.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 4, "memory": "5Gi"},
                                                limits={"cpu": 6, "memory": "8Gi"})
        },
        "text_labeling_pipeline": {
            "args": {
                "sentiment_analysis_model_path": f"{models_pfx}/models/probabilistic_language_model/gpt2/",
                "sentence_simplification_path": f"{models_pfx}/models/sentence_simplification/bert_srl/",
                "input_s3_loc": f"{data_pfx}/text_fixing_pipeline/{run_date}/:app_os_app_reviews_text_fixing.csv",
                "output_s3_loc": (f"{data_pfx}/text_labeling_pipeline/{run_date}/"
                                  f":app_os_app_reviews_text_labeling.csv")
            },
            "resources": V1ResourceRequirements(requests={"cpu": 4, "memory": "4Gi"},
                                                limits={"cpu": 5, "memory": "6Gi"})
        },
        "text_filtering_pipeline": {
            "args": {
                "input_s3_loc": f"{data_pfx}/text_labeling_pipeline/{run_date}/:app_os_app_reviews_text_labeling.csv",
                "output_s3_loc": (f"{data_pfx}/text_filtering_pipeline/{run_date}/"
                                  f":app_os_app_reviews_text_filtering.csv")
            },
            "resources": V1ResourceRequirements(requests={"cpu": 1, "memory": "1Gi"},
                                                limits={"cpu": 2, "memory": "2Gi"})
        },
        "topic_labeling_pipeline": {
            "args": {
                "prompt_topic_model_path": f"{models_pfx}/models/text_classification/prompt_topic_classification/",
                "base_model_path": f"{models_pfx}/models/probabilistic_language_model/flan_t5_base/",
                "base_model_name": "t5",
                "taxonomy_matching_model_path": (f"{models_pfx}/models/taxonomy_matching/"
                                                 "topic_taxonomy_matching_no_ambiguous/"),
                "input_s3_loc": f"{data_pfx}/text_filtering_pipeline/{run_date}/:app_os_app_reviews_text_filtering.csv",
                "output_s3_loc": f"{data_pfx}/topic_labeling_pipeline/{run_date}/:app_os_app_reviews_topic_labeling.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 6, "memory": "5Gi"},
                                                limits={"cpu": 8, "memory": "7Gi"})
        },
        "issue_labeling_pipeline": {
            "args": {
                "taxonomy_matching_model_path": f"{models_pfx}/models/taxonomy_matching/topic_taxonomy_matching/",
                "aspect_sentiment_model_path": f"{models_pfx}/models/aspect_sentiment_analysis/deberta/",
                "input_s3_loc": f"{data_pfx}/topic_labeling_pipeline/{run_date}/:app_os_app_reviews_topic_labeling.csv",
                "output_s3_loc": f"{data_pfx}/issue_labeling_pipeline/{run_date}/:app_os_app_reviews_issue_labeling.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 3, "memory": "3Gi"},
                                                limits={"cpu": 4, "memory": "5Gi"})
        },
        "calling_pipeline": {
            "args": {
                "prompt_calling_model_path": (f"{models_pfx}/models/text_classification/"
                                              "prompt_calling_issue_classification/"),
                "base_model_path": f"{models_pfx}/models/probabilistic_language_model/t5_base/",
                "base_model_name": "t5",
                "taxonomy_calling_model_path": (f"{models_pfx}/models/taxonomy_matching/"
                                                f"calling_issue_taxonomy_matching/"),
                "aspect_sentiment_model_path": f"{models_pfx}/models/aspect_sentiment_analysis/deberta/",
                "input_s3_loc": f"{data_pfx}/issue_labeling_pipeline/{run_date}/:app_os_app_reviews_issue_labeling.csv",
                "output_s3_loc": f"{data_pfx}/calling_pipeline/{run_date}/:app_os_app_reviews_calling_pipeline.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 2, "memory": "4Gi"},
                                                limits={"cpu": 4, "memory": "6Gi"})
        },
        "messaging_pipeline": {
            "args": {
                "prompt_messaging_model_path": (f"{models_pfx}/models/text_classification/"
                                                "prompt_messaging_issue_classification/"),
                "base_model_path": f"{models_pfx}/models/probabilistic_language_model/t5_base/",
                "base_model_name": "t5",
                "taxonomy_messaging_model_path": (f"{models_pfx}/models/taxonomy_matching/"
                                                  "messaging_issue_taxonomy_matching/"),
                "aspect_sentiment_model_path": f"{models_pfx}/models/aspect_sentiment_analysis/deberta/",
                "input_s3_loc": f"{data_pfx}/calling_pipeline/{run_date}/:app_os_app_reviews_calling_pipeline.csv",
                "output_s3_loc": f"{data_pfx}/messaging_pipeline/{run_date}/:app_os_app_reviews_messaging_pipeline.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 2, "memory": "4Gi"},
                                                limits={"cpu": 3, "memory": "6Gi"})
        },
        "app_pipeline": {
            "args": {
                "prompt_app_model_path": f"{models_pfx}/models/text_classification/prompt_app_issue_classification/",
                "base_model_path": f"{models_pfx}/models/probabilistic_language_model/flan_t5_base/",
                "base_model_name": "t5",
                "taxonomy_app_model_path": f"{models_pfx}/models/taxonomy_matching/app_issue_taxonomy_matching/",
                "aspect_sentiment_model_path": f"{models_pfx}/models/aspect_sentiment_analysis/deberta/",
                "input_s3_loc": f"{data_pfx}/messaging_pipeline/{run_date}/:app_os_app_reviews_messaging_pipeline.csv",
                "output_s3_loc": f"{data_pfx}/app_pipeline/{run_date}/:app_os_app_reviews_app_pipeline.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 2, "memory": "5Gi"},
                                                limits={"cpu": 4, "memory": "7Gi"})
        },
        "cost_pipeline": {
            "args": {
                "prompt_cost_model_path": f"{models_pfx}/models/text_classification/prompt_cost_issue_classification/",
                "base_model_path": f"{models_pfx}/models/probabilistic_language_model/flan_t5_base/",
                "base_model_name": "t5",
                "taxonomy_cost_model_path": f"{models_pfx}/models/taxonomy_matching/cost_issue_taxonomy_matching/",
                "aspect_sentiment_model_path": f"{models_pfx}/models/aspect_sentiment_analysis/deberta/",
                "input_s3_loc": f"{data_pfx}/app_pipeline/{run_date}/:app_os_app_reviews_app_pipeline.csv",
                "output_s3_loc": f"{data_pfx}/cost_pipeline/{run_date}/:app_os_app_reviews_cost_pipeline.csv"
            },
            "resources": V1ResourceRequirements(requests={"cpu": 2, "memory": "5Gi"},
                                                limits={"cpu": 4, "memory": "7Gi"})
        }
    }


def get_args_list(pipeline_module: str, args: dict, app_os: str):
    args_list = ["python", pipeline_module]
    for parameter, value in args.items():
        args_list.append(f'--{parameter}')
        args_list.append(value if parameter == 'base_model_name' else value.replace(':app_os', app_os))
    return args_list


app_reviews_classification()

if __name__ == "__main__":
    app_reviews_classification().test(execution_date="2023-10-31")
    # app_reviews_classification().test_dq_checks(execution_date="2023-06-26")
