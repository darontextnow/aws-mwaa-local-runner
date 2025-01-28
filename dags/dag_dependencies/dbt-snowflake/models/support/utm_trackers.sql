{{
    config(
        tags=['4h'],
        materialized='incremental',
        unique_key=['client_type', 'tracker'],
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT DISTINCT
    client_type,
    UPPER(LEFT(MD5(
        utm_source
        || COALESCE(utm_medium, '')
        || COALESCE(utm_term, '')
        || COALESCE(utm_content, '')
        || COALESCE(utm_campaign, '')
    ), 6)) AS tracker,
    utm_source::VARCHAR(256) AS network_name,
    utm_campaign AS campaign_name,
    COALESCE(utm_term, utm_medium) AS adgroup_name,
    utm_content AS creative_name
FROM {{ ref('registrations_1') }}
WHERE
    (utm_source IS NOT NULL)
    AND (created_at BETWEEN {{ var('data_interval_start') }}::TIMESTAMP
        AND {{ var('data_interval_end') }}::TIMESTAMP)
