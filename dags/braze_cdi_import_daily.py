"""DAG to send attributes and events to Braze
This DAG runs daily after dbt completion, it prepares the data and sync the data to Braze.
"""
from airflow_utils import dag
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_WH_MEDIUM, SF_WH_SMALL
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 5, 16),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_on_failure,
        "retries": 1
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/braze",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def braze_cdi_import_daily():
    insert_user_event = SnowflakeAsyncDeferredOperator(
        task_id='insert_user_events',
        sql="user_event_daily.sql",
        warehouse=SF_WH_MEDIUM
    )

    insert_users_events_sync = SnowflakeAsyncDeferredOperator(
        task_id="sync_users_events",
        sql="""INSERT INTO braze_cloud_production.ingestion.users_events_sync (updated_at, external_id, payload)
            SELECT 
                CURRENT_TIMESTAMP, 
                user_id_hex AS external_id, 
                OBJECT_CONSTRUCT('name', event_name, 'time', event_time)::VARCHAR AS payload
            FROM ua.user_events_braze_import
            WHERE
                (inserted_at = '{{ ts }}')
                AND (event_name IN ('PrimaryUserEntered', 'PrimaryUserExited', 'PrimaryUserEnteredWeek2'
                        , 'PixalateFlagged'))""",
        warehouse=SF_WH_SMALL
    )

    insert_pu_attribute = SnowflakeAsyncDeferredOperator(
        task_id='insert_primary_user_attribute',
        sql="primary_user_attribute.sql",
        warehouse=SF_WH_MEDIUM
    )

    insert_daily_attribute = SnowflakeAsyncDeferredOperator(
        task_id='insert_daily_user_attribute',
        sql="user_attribute_daily.sql",
        warehouse=SF_WH_MEDIUM
    )

    insert_users_attributes_sync = SnowflakeAsyncDeferredOperator(
        task_id="sync_users_attributes",
        sql="""INSERT INTO braze_cloud_production.ingestion.users_attributes_sync (updated_at, external_id, payload)
            SELECT
                 CURRENT_TIMESTAMP, 
                 user_id_hex AS EXTERNAL_ID,
                 OBJECT_CONSTRUCT ( 
                     'email', email,
                     'first_name', first_name,
                     'gender', gender,
                     'PredictedEthnicity', PredictedEthnicity,
                     'UserAgeRange', UserAgeRange,
                     'PixalateFraudType', PixalateFraudType,
                     'PixalateProbability', TO_DECIMAL(PixalateProbability, 11, 10),
                     'PrimaryUser', TO_BOOLEAN(PrimaryUser)
                 )::VARCHAR AS PAYLOAD 
            FROM (
                SELECT user_id_hex, username, attr_name, attr_value
                FROM ua.user_attributes_braze_import
                WHERE attr_name IN (
                    'email',
                    'first_name',
                    'gender',                    
                    'PredictedEthnicity', 
                    'UserAgeRange', 
                    'PixalateProbability', 
                    'PixalateFraudType',
                    'PrimaryUser'
                )
                AND inserted_at = '{{ ts }}'
            ) a 
            PIVOT (MAX(attr_value) FOR attr_name IN ( 
                'email',
                'first_name',
                'gender',
                'PredictedEthnicity',
                'UserAgeRange',
                'PixalateProbability',
                'PixalateFraudType',
                'PrimaryUser'
            )) AS p 
            (
             user_id_hex,
             username,
             email,
             first_name,
             gender,             
             PredictedEthnicity, 
             UserAgeRange, 
             PixalateProbability, 
             PixalateFraudType,
             PrimaryUser                
            ) 
        WHERE payload <> PARSE_JSON('{}') 
        UNION ALL
        SELECT    CURRENT_TIMESTAMP, user_id_hex AS EXTERNAL_ID,
             OBJECT_CONSTRUCT_KEEP_NULL('IapBundles', STRTOK_TO_ARRAY(attr_value,','))::VARCHAR AS PAYLOAD 
        FROM ua.user_attributes_braze_import
        WHERE attr_name = 'IapBundles' 
            AND inserted_at = '{{ ts }}'
            AND payload <> PARSE_JSON('{}') 
        """,
        warehouse=SF_WH_SMALL
    )

    insert_profile_complete_status = SnowflakeAsyncDeferredOperator(
        task_id="insert_profile_complete_status",
        sql="""MERGE INTO braze_cloud_production.ingestion.user_profile_completion_status tgt
                USING (
                        WITH active_user AS 
                        (
                            SELECT u.user_id_hex, max(active_date) as active_date
                            FROM prod.dau.username_active_days a
                            INNER JOIN prod.core.users u ON u.username = a.username
                            WHERE a.active_date = '{{ ds }}' and u.account_status NOT IN ('DISABLED', 'HARD_DISABLED') 
                            GROUP BY 1    
                        )                             
                        ,user_profile AS (
                        SELECT u.user_id_hex,active_date,
                            SUM( 
                                (CASE WHEN p.first_name IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.last_name IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN COALESCE(p.use_cases,p.other_use_cases) IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.age_range IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.interests IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.gender IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.ethnicity IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.household_income IS NULL THEN 0 ELSE 1 END) + 
                                (CASE WHEN p.zip_code IS NULL THEN 0 ELSE 1 END)         
                            ) AS field_count    
                        FROM active_user u
                        LEFT JOIN  prod.product_analytics.user_account_profile p ON u.user_id_hex = p.user_id_hex
                        GROUP BY 1,2 )
                        SELECT user_id_hex AS external_user_id, 
                            CASE WHEN field_count = 9 THEN 'COMPLETE' 
                                WHEN field_count > 0 AND FIELD_COUNT < 9 THEN 'PARTIAL'
                                WHEN field_count = 0 THEN 'BLANK' END AS status,active_date
                        FROM user_profile ) src ON tgt.external_user_id = src.external_user_id  
                WHEN MATCHED THEN 
                UPDATE SET
                   tgt.status = src.status,
                   tgt.last_active_date = src.active_date,
                   tgt.updated_at = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN
                 INSERT
                   (external_user_id, status, last_active_date, inserted_at, updated_at)
                 VALUES
                 (src.external_user_id, src.status, active_date, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
        warehouse=SF_WH_MEDIUM
    )

    (insert_user_event >> insert_users_events_sync >> insert_pu_attribute >>
     insert_daily_attribute >> insert_users_attributes_sync >> insert_profile_complete_status)


braze_cdi_import_daily()

if __name__ == "__main__":
    braze_cdi_import_daily().test(execution_date="2024-06-13")
