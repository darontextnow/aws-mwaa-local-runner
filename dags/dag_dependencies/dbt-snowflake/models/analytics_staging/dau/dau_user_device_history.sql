{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH data_available_till_date AS (
    SELECT MAX(date_utc) AS processed_date FROM {{ source('dau', 'user_device_master') }}
),
history_last_14_days AS (
    SELECT
        username,
        adid,
        COUNT(DISTINCT date_utc) num_days_seen_last_14_days,
        COUNT(DISTINCT source) num_sources,
        MAX(CASE WHEN source = 'adjust.registrations' THEN 1 ELSE 0 END) seen_at_registration_flag,
        MAX(date_utc) last_date_seen,
        MIN(date_utc) first_date_seen,
        {{ dbt_utils.generate_surrogate_key(['username', 'adid']) }} AS surrogate_key
    FROM {{ source('dau', 'user_device_master') }}
    WHERE (date_utc > (SELECT processed_date - 14 FROM data_available_till_date))
    GROUP BY 1, 2
)

{% if is_incremental() %}
    ,
    history_from_last_processed_minus_14_days AS (
        SELECT
            username,
            adid,
            COUNT(DISTINCT date_utc) num_days_seen,
            COUNT(DISTINCT source) num_sources,
            ARRAY_AGG(DISTINCT source) WITHIN GROUP (ORDER BY source) sources_info,
            MAX(CASE WHEN source = 'adjust.registrations' THEN 1 ELSE 0 END) seen_at_registration_flag,
            MAX(date_utc) last_date_seen,
            MIN(date_utc) first_date_seen,
            {{ dbt_utils.generate_surrogate_key(['username', 'adid']) }} AS surrogate_key
        FROM {{ source('dau', 'user_device_master') }}
        WHERE (date_utc > (SELECT MAX(processed_date) -14 FROM {{ this }}))
        GROUP BY 1, 2
    )

    SELECT 
        hist_lp_minus_14_days.username AS username,
        hist_lp_minus_14_days.adid AS adid,
        hist_lp_minus_14_days.last_date_seen AS last_date_seen,
        NVL(base.first_date_seen, hist_lp_minus_14_days.first_date_seen) AS first_date_seen,
        base.num_days_seen - base.num_days_seen_last_14_days + hist_lp_minus_14_days.num_days_seen AS num_days_seen,
        ARRAY_SIZE(admin.ARRAY_FLAT_DISTINCT( ARRAY_CAT(NVL(base.sources_info, array_construct()),hist_lp_minus_14_days.sources_info ))) AS num_sources,
        GREATEST(base.seen_at_registration_flag, hist_lp_minus_14_days.seen_at_registration_flag ) AS seen_at_registration_flag,
        data_available_till_date.processed_date AS processed_date,
        hist_last_14_days.num_days_seen_last_14_days AS num_days_seen_last_14_days,
        admin.ARRAY_FLAT_DISTINCT( ARRAY_CAT(NVL(base.sources_info, array_construct()),hist_lp_minus_14_days.sources_info )) AS sources_info,
        hist_lp_minus_14_days.surrogate_key AS surrogate_key
    FROM history_from_last_processed_minus_14_days hist_lp_minus_14_days
    LEFT JOIN {{ this }} base ON (base.surrogate_key = hist_lp_minus_14_days.surrogate_key)
    LEFT JOIN history_last_14_days hist_last_14_days ON (hist_lp_minus_14_days.surrogate_key  = hist_last_14_days.surrogate_key)
    CROSS JOIN data_available_till_date

{% else %}

    SELECT
        full_history.username AS username,
        full_history.adid AS adid,
        full_history.num_days_seen AS num_days_seen,
        full_history.num_sources AS num_sources ,
        full_history.seen_at_registration_flag,
        full_history.last_date_seen  AS last_date_seen,
        full_history.first_date_seen AS first_date_seen,
        data_available_till_date.processed_date AS processed_date,
        NVL(last_14.num_days_seen_last_14_days,0) AS num_days_seen_last_14_days,
        full_history.sources_info AS sources_info,
        full_history.surrogate_key AS surrogate_key
    FROM (
        SELECT
            username,
            adid,
            COUNT(DISTINCT date_utc) num_days_seen,
            COUNT(DISTINCT source) num_sources,
            MAX(CASE WHEN source='adjust.registrations' THEN 1 ELSE 0 END) seen_at_registration_flag,
            MAX(date_utc) last_date_seen,
            MIN(date_utc) first_date_seen,
            ARRAY_AGG(DISTINCT source) WITHIN GROUP (ORDER BY source) sources_info,
            {{ dbt_utils.generate_surrogate_key(['username', 'adid']) }} AS surrogate_key
        FROM {{ source('dau', 'user_device_master') }}
        GROUP BY 1, 2
    ) full_history
    LEFT JOIN history_last_14_days last_14 ON (full_history.surrogate_key = last_14.surrogate_key)
    CROSS JOIN data_available_till_date

{% endif %}

--14 days of past data is getting calculated every run, since we are dealing with the data that can get a record with a past date
-- This is idempodent
-- supports data backfilling
-- Custom snowflake function (ARRAY_FLAT_DISTINCT) which takes an array and returns distinctive elements from an array
-- if we need to back fill the data in the user_device_master, we need to full refresh this model
--  four additional columns
         -- num_days_seen_last_14_days (no_of_days seen in the last 14 days from the latest processed date)
         -- sources_info ( array of the sources, there are only five different types of sources, so storing it in an array )
         -- processed_date  (latest processed date )
         -- surrogate key