{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH set_client_type AS (
    SELECT date_utc, set_uuid, client_type
    FROM {{ source('dau', 'user_set') }}
    JOIN {{ source('dau', 'user_device_master') }} USING (username)
    WHERE
        (date_utc < {{ var('current_date') }})
    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}

    UNION SELECT date_utc, set_uuid, client_type
    FROM {{ source('dau', 'device_set') }}
    JOIN {{ source('dau', 'user_device_master') }} USING (adid)
    WHERE
        (date_utc < {{ var('current_date') }})
    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}
)

SELECT
    date_utc,
    set_uuid AS user_set_id,
    client_type,
    cohort_utc,
    DATEDIFF('DAY', cohort_utc, date_utc) AS day_from_cohort,
    SUM(dau) AS dau
FROM {{ source('dau', 'daily_active_users') }}
JOIN set_client_type USING (date_utc, set_uuid)
JOIN {{ source('dau', 'set_cohort') }} USING (set_uuid, client_type)

{% if is_incremental() %}
    WHERE (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
{% endif %}

GROUP BY 1, 2, 3, 4, 5
ORDER BY 1
