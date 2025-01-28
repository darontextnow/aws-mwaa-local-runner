{{
    config(
        tags=['4h'],
        materialized='incremental',
        incremental_strategy='append',
        snowflake_warehouse='PROD_WH_LARGE',
        pre_hook=[
            "
            /* This insert of sessions from ppr.applifecyclechanged needs be commited
                  before the other sources (below in main script) run their inserts
                  as we only want to insert the other sources if username does not already exist from this source.
               We are inserting all DISTINCT sessions here.
               Note we have to escape the double quotes below because this query is wrapped by a double quote.
            */
            INSERT INTO {{ this }} SELECT DISTINCT -- there are some duplicates in source table
                pp.created_at,
                'client' AS event_source,
                TRIM(\"client_details.client_data.user_data.username\") AS username,
                CASE
                    WHEN (\"client_details.client_data.client_platform\" = 'IOS') THEN 'TN_IOS_FREE'
                    WHEN (\"client_details.client_data.client_platform\" = 'CP_ANDROID')
                        AND (\"client_details.client_data.brand\" = 'BRAND_2NDLINE') THEN '2L_ANDROID'
                    WHEN (\"client_details.client_data.client_platform\" = 'CP_ANDROID')
                        AND (\"client_details.client_data.brand\" = 'BRAND_TEXTNOW') THEN 'TN_ANDROID'
                END AS client_type,
                \"client_details.client_data.client_version\" AS client_version,
                \"client_details.client_data.country_code\" AS country_code,
                NULL AS city_name,
                NULL AS latitude,
                NULL AS longitude,
                \"client_details.client_data.client_ip_address\" AS client_ip,
                NULL AS phone_num,
                COALESCE(\"client_details.android_bonus_data.adjust_id\",
                         \"client_details.ios_bonus_data.adjust_id\"
                ) AS adid,
                NULL AS tracker,
                NULL AS installed_at,
                NULL AS imputed_timestamp,
                'party_planner_realtime.applifecyclechanged' AS data_source
            FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }} AS pp
            LEFT JOIN (
                SELECT created_at, username, client_type
                FROM {{ this }}
                WHERE
                    (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '4 DAY')
                    AND (created_at <= {{ var('data_interval_end') }}::TIMESTAMP_NTZ)
                    AND (data_source = 'party_planner_realtime.applifecyclechanged')
            ) AS s ON
                (pp.created_at = s.created_at)
                AND (username = s.username)
                AND (client_type = s.client_type)
            WHERE
                (pp.created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '4 DAY') -- 5 day total lookback
                AND (pp.created_at <= {{ var('current_timestamp') }}::TIMESTAMP_NTZ) -- realtime source so load to current
                AND (pp.\"payload.app_lifecycle\" = 'APP_LIFECYCLE_FOREGROUNDED') -- only include session starts, not end;
                AND (TRIM(NVL(pp.\"client_details.client_data.user_data.username\", '')) <> '')
                AND (s.username IS NULL) --only insert new sessions not already inserted before.
            "
        ]
    )
}}

--INSERT one row per day from additional sources if user doesn't show up in party_planner insert above.
SELECT
    created_at,
    event_source,
    username::VARCHAR(256) AS username,
    client_type::VARCHAR(256) AS client_type,
    client_version,
    country_code,
    city_name,
    latitude::DECIMAL(11,9) AS latitude,
    longitude::DECIMAL(12,9) AS longitude,
    NULL::VARCHAR(39) AS client_ip,
    NULL::VARCHAR(100) AS phone_num,
    NULL::VARCHAR(256) AS adid,
    NULL::VARCHAR(256) AS tracker,
    NULL AS installed_at,
    NULL::BOOLEAN AS imputed_timestamp,
    data_source AS data_source
FROM (
    -- Sessions through core.messages - INSERT one row for each day in latest two days
    SELECT DISTINCT
        --next line gives us the min created_at for each day of the two day window
        MIN(created_at) OVER (PARTITION BY username, client_type, DATE(created_at)) AS created_at,
        'server' AS event_source,
        'core.messages' AS data_source,
        username,
        client_type,
        FIRST_VALUE(client_version) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS client_version,
        FIRST_VALUE(country_code) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS country_code,
        FIRST_VALUE(city_name) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS city_name,
        FIRST_VALUE(latitude) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latitude,
        FIRST_VALUE(longitude) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS longitude
    FROM {{ source('core', 'messages') }}
    WHERE
        (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 DAY')
        AND (created_at < {{ var('ds') }}::TIMESTAMP_NTZ + INTERVAL '1 DAY')
        AND (TRIM(NVL(username, '')) <> '')
        --next filter removes all rows for user that have already been inserted for each day in window
        AND (CONCAT(TRIM(NVL(username, '')), DATE(created_at)) NOT IN (
            SELECT DISTINCT CONCAT(username, DATE(created_at))
            FROM {{ this }}
            WHERE
                (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 DAY')
                AND (created_at < {{ var('ds') }}::TIMESTAMP_NTZ + INTERVAL '1 DAY')
                AND (TRIM(NVL(username, '')) <> '')
        ))

    -- Sessions through core.registrations - INSERT one row for each day in latest two days
    UNION ALL SELECT DISTINCT
        --next line gives us the min created_at for each day of the two day window
        MIN(created_at) OVER (PARTITION BY username, client_type, DATE(created_at)) AS created_at,
        'server' AS event_source,
        'core.registrations' AS data_source,
        username,
        client_type,
        FIRST_VALUE(client_version) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS client_version,
        FIRST_VALUE(country_code) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS country_code,
        FIRST_VALUE(city_name) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS city_name,
        FIRST_VALUE(latitude) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latitude,
        FIRST_VALUE(longitude) OVER (PARTITION BY username, client_type, DATE(created_at) ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS longitude
    FROM {{ ref('registrations_1') }}
    WHERE
        (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 DAY')
                AND (created_at < {{ var('ds') }}::TIMESTAMP_NTZ + INTERVAL '1 DAY')
        AND (NVL(tracker_name, '') NOT LIKE 'Untrusted Devices%')
        --next filter removes all rows for user that have already been inserted for each day in window
        AND (CONCAT(TRIM(NVL(username, '')), DATE(created_at)) NOT IN (
            SELECT DISTINCT CONCAT(username, DATE(created_at))
            FROM {{ this }}
            WHERE
                (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 DAY')
                AND (created_at < {{ var('ds') }}::TIMESTAMP_NTZ + INTERVAL '1 DAY')
                AND (TRIM(NVL(username, '')) <> '')
        ))

    -- Sessions through partyplanner.adshoweffective - INSERT one row for each day in latest two days
    UNION ALL SELECT
        MIN(created_at) AS created_at,
        'client' AS event_source,
        'partyplanner_realtime.adshoweffective' AS data_source,
        "client_details.client_data.user_data.username" AS username,
        {{ pp_normalized_client_type() }} AS client_type,
        "client_details.client_data.client_version" AS client_version,
        "client_details.client_data.country_code" AS country_code,
        NULL AS city_name,
        NULL AS latitude,
        NULL AS longitude
    FROM {{ ref('adshoweffective_combined_data') }}
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '1 DAY' AND {{ var('ds') }}::DATE)
        AND (TRIM(NVL(username, '')) <> '')
        --next filter removes all rows for user that have already been inserted for each day in window
        AND (CONCAT(TRIM(NVL(username, '')), date_utc) NOT IN (
            SELECT DISTINCT CONCAT(username, DATE(created_at)) FROM {{ this }}
            WHERE
                (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 DAY')
                AND (created_at < {{ var('ds') }}::TIMESTAMP_NTZ + INTERVAL '1 DAY')
                AND (TRIM(NVL(username, '')) <> '')
        ))
    GROUP BY date_utc, username, client_type, client_version, country_code
)
