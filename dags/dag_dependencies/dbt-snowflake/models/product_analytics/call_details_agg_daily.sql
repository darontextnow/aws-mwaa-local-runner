{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
      
    )
}}

WITH variables AS (
    SELECT
        2000 AS established_after,
        15000 AS eligible_for_rating,
        500 AS audio_should_start,
        42 AS days_of_history,
        --NULL AS max_call_duration,
        4 * 3600000 AS max_call_duration,
        0 AS count_NULL_packets_as_no_audio, -- 0 means true here
        3.0 AS low_mos
),
user_data AS (
    SELECT DISTINCT
    a.date_utc,
    a.username,
    a.client_type,
    COALESCE(c.segment, 'New User/Unknown Segment') AS user_segment,
    application_version AS app_version,
    a.call_direction,
    CASE WHEN call_rating IS NOT NULL AND call_rating > 0 THEN call_rating ELSE NULL END AS valid_call_rating,
    COUNT(CASE WHEN NVL(call_duration, 0) <= 0 THEN 1 END) AS count_zero_or_not_established,
    COUNT(CASE WHEN call_duration >= 1 THEN 1 END) AS count_not_zero,
    COUNT(CASE WHEN call_duration >= (SELECT established_after FROM variables) THEN 1 END) AS count_started,
    COUNT(CASE WHEN call_duration >= (SELECT eligible_for_rating FROM variables) THEN 1 END) AS count_eligilbe_for_survey,
    CASE WHEN call_duration >= (SELECT established_after FROM variables) 
        AND computed_mos >= 0 THEN computed_mos END AS mos,
    CASE WHEN call_duration >= (SELECT established_after FROM variables) 
        AND computed_mos >= 0 AND computed_mos < (SELECT low_mos FROM variables) THEN 1 END AS flag_bad_mos,
    CASE WHEN call_duration >= (SELECT established_after FROM variables) THEN call_duration::FLOAT/1000 END AS elig_call_duration_sec,
    CASE WHEN call_duration >= (SELECT established_after FROM variables) THEN max_jitter END AS elig_max_jitter,
    CASE WHEN call_duration >= (SELECT established_after FROM variables) THEN packet_loss END AS elig_packet_loss,
    CASE WHEN call_duration >= 1 THEN call_duration::FLOAT/1000 END AS elig_any_call_duration,
    COUNT(CASE WHEN call_duration >= (SELECT established_after FROM variables) 
        AND NVL(packets_sent, (SELECT count_NULL_packets_as_no_audio FROM variables)) = 0 THEN 1 END) AS count_no_sent_audio,
    COUNT(CASE WHEN call_duration >= (SELECT established_after FROM variables)
        AND NVL(packets_received, (SELECT count_NULL_packets_as_no_audio FROM variables)) = 0 THEN 1 END) AS count_no_received_audio,
    CASE WHEN elig_call_duration_sec between 0 AND 2 THEN 1 ELSE NULL END AS flag_short_call,
    FROM {{ ref('legacy_call_end') }} a 
    LEFT JOIN {{ ref('analytics_users') }} b ON b.user_id_hex = a.user_id_hex
    LEFT JOIN {{ source('analytics', 'segmentation') }} c ON 
        (c.user_set_id = b.user_set_id) 
        AND (c.score_date = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1))
    LEFT JOIN (
        SELECT call_id, call_duration, call_rating
        FROM {{ ref('call_ratings_combined_sources') }}
        WHERE
            (date_utc >= {{ var('ds') }}::DATE - INTERVAL '30 DAYS')
            AND (date_utc < CURRENT_DATE)
    ) d USING (call_id)
    WHERE
    {% if is_incremental() or target.name == 'dev' %}
        (a.date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (a.date_utc >= '2022-09-01')
    {% endif %}
        AND (a.date_utc < CURRENT_DATE)
        AND (a.client_type IN ('TN_ANDROID', 'TN_IOS_FREE'))
    GROUP BY 1, 2, 3, 4, 5, 6, call_rating, call_duration, computed_mos, max_jitter, packet_loss
),
agg_user AS (
    SELECT
        date_utc, 
        username,
        client_type,
        user_segment AS segment,
        app_version,
        call_direction,
        AVG(valid_call_rating) AS avg_call_rating,
        SUM(count_zero_or_not_established) AS count_zero_or_not_established,
        SUM(count_not_zero) AS count_not_zero,
        SUM(count_eligilbe_for_survey) AS count_eligilbe_for_survey,
        SUM(count_started) AS count_call_started,
        AVG(mos) AS avg_mos,
        SUM(flag_bad_mos) AS count_bad_mos,
        AVG(elig_call_duration_sec) AS avg_call_duration,
        AVG(elig_max_jitter) AS avg_max_jitter,
        AVG(elig_packet_loss) AS avg_packet_loss,
        AVG(elig_any_call_duration) AS avg_any_call_duration,
        SUM(count_no_sent_audio) AS count_no_sent_audio,
        SUM(count_no_received_audio) AS count_no_received_audio,
        SUM(flag_short_call) AS count_short_calls
    FROM user_data
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    date_utc, 
    client_type,
    segment,
    app_version,
    call_direction,
    COUNT(DISTINCT username) AS cnt_users,
    AVG(avg_call_rating) AS avg_call_rating,
    SUM(count_zero_or_not_established) AS count_zero_or_not_established, 
    SUM(count_not_zero) AS count_not_zero,
    SUM(count_eligilbe_for_survey) AS count_eligilbe_for_survey,
    SUM(count_call_started) AS count_call_started,
    AVG(avg_mos) AS avg_mos,
    SUM(count_bad_mos) AS count_bad_mos,
    AVG(avg_call_duration) AS avg_call_duration,
    AVG(avg_max_jitter) AS avg_max_jitter,
    AVG(avg_packet_loss) AS avg_packet_loss,
    AVG(avg_any_call_duration) AS avg_any_call_duration,
    SUM(count_no_sent_audio) AS count_no_sent_audio,
    SUM(count_no_received_audio) AS count_no_received_audio,
    SUM(count_short_calls) AS count_short_calls
FROM agg_user
GROUP BY 1, 2, 3, 4, 5
