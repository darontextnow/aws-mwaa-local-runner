{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='user_id_hex'
    )
}}

/*

It appears that there is a bug with PP data where the one hex id can be
associated with multiple users, and one user can be associated with multiple hex ids.

Until that is fixed, we are using the mapping approach for now

 */

WITH email_status_log AS (

    SELECT NULLIF("client_details.client_data.user_data.email_status", 'EMAIL_STATUS_UNKNOWN') AS email_status,
           "client_details.client_data.user_data.username" AS username,
           created_at as received_at
    FROM {{ source('party_planner_realtime', 'registrations') }}
    WHERE email_status IS NOT NULL
      AND username IS NOT NULL
    {% if is_incremental() %}

      AND received_at >= (SELECT MAX(updated_at)::date - interval '7 days' FROM {{ this }})

    {% endif %}

    UNION ALL

    SELECT NULLIF("client_details.client_data.user_data.email_status", 'EMAIL_STATUS_UNKNOWN') AS email_status,
           "client_details.client_data.user_data.username" AS username,
           created_at as received_at
    FROM {{ source('party_planner_realtime', 'user_updates') }}
    WHERE email_status IS NOT NULL
      AND username IS NOT NULL
    {% if is_incremental() %}

      AND received_at >= (SELECT MAX(updated_at)::date - interval '7 days' FROM {{ this }})

    {% endif %}
    ORDER BY username, received_at

),

last_email_status AS (

    SELECT DISTINCT
        username,
        LAST_VALUE(email_status) OVER
          (PARTITION BY username ORDER BY received_at
           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS email_status,
        MAX(received_at) OVER (PARTITION BY username)        AS updated_at
    FROM email_status_log
    WHERE email_status IS NOT NULL

)

SELECT
    user_id_hex,
    username,
    email_status,
    updated_at
FROM last_email_status
JOIN {{ ref('analytics_users') }} USING (username)