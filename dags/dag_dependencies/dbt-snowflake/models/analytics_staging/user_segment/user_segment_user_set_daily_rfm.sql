{{
    config(
        tags=['daily'],
        materialized='view',
        bind=False,
        unique_key='date_utc || user_set_id'
    )
}}

SELECT
    run_date as date_utc,
    set_uuid as user_set_id,
    rfm_segment
FROM {{ source('user_segment', 'rfm_segments') }}