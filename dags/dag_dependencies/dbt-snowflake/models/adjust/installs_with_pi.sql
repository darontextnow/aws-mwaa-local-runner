{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_LARGE',
        pre_hook = [
            "--Delete all duplicate records by snow pipe since last run.
            --The first install record will be reinserted from adjust_installs_with_pi_staging table below.
            DELETE FROM {{ this }} tgt
            USING {{ ref('adjust_duplicate_installs_staging') }} src
            WHERE (tgt.adid = src.adid) AND (tgt.app_id = src.app_id)"
        ]
    )
}}

--INSERT only the first, new install (by adid, app_id) into adjust.installs_with_pi
--additional (duplicates by adid, app_id) will be inserted into sessions_with_pi and reattributions_with_pi tables
SELECT * FROM {{ ref('adjust_duplicate_installs_staging') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY adid, app_id ORDER BY created_at, environment NULLS LAST) = 1
