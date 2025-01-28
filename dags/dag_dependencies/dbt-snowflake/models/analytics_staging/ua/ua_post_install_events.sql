{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key'
    )
}}

WITH base AS (
    SELECT
        surrogate_key,
        client_type,
        adjust_id,
        installed_at,
        MAX(CASE WHEN day_from_install = 3 THEN 1 ELSE 0 END) ::SMALLINT active_on_day_3,
        COUNT(DISTINCT CASE WHEN day_from_install BETWEEN 0 AND 2 THEN day_from_install END)::SMALLINT AS active_days_first_3days,
        COUNT(DISTINCT CASE WHEN day_from_install BETWEEN 0 AND 4 THEN day_from_install END)::SMALLINT AS active_days_first_5days,
        COUNT(DISTINCT CASE WHEN day_from_install BETWEEN 22 AND 28 THEN day_from_install END)::SMALLINT AS active_days_week4
    FROM {{ ref('adjust_installs') }}
    JOIN {{ ref('adjust_install_active_days') }} USING (app_id, installed_at)
    WHERE

    {% if is_incremental() %}
        (installed_at >= {{ var('ds') }}::DATE - INTERVAL '30 DAYS')
        AND (active_date < CURRENT_DATE)
    {% else %}
        (installed_at BETWEEN '2020-08-01'::DATE AND {{ var('ds') }}::DATE)
        AND (active_date >= '2020-08-01'::DATE)
    {% endif %}

        AND (adid = adjust_id)
        AND (day_from_install <= 28)
        AND (NOT is_untrusted)
    GROUP BY 1, 2, 3, 4
),

permission_states AS (
    WITH perms AS (
        SELECT
            client_type,
            "client_details.android_bonus_data.adjust_id" AS adjust_id,
            "payload.permission_type" AS permission_type,
            MAX(CASE "payload.permission_alert_state"
                WHEN 'ALERT_STATE_SHOWN' THEN 0
                WHEN 'ALERT_STATE_ACCEPTED' THEN 1
                WHEN 'ALERT_STATE_DENIED' THEN -1
            END) AS permission_state
        FROM {{ source('party_planner_realtime', 'permission') }}
        JOIN base ON
            (permission."client_details.android_bonus_data.adjust_id" = adjust_id)
            AND (CASE permission."client_details.client_data.brand"
                WHEN 'BRAND_TEXTNOW' THEN 'TN_ANDROID'
                WHEN 'BRAND_2NDLINE' THEN '2L_ANDROID'
            END = base.client_type)
        WHERE

        {% if is_incremental() %}
            (created_at >= {{ var('ds') }}::DATE - INTERVAL '31 DAYS')
        {% else %}
            (created_at BETWEEN '2020-08-01'::DATE AND {{ var('ds') }}::DATE)
        {% endif %}

            AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
            AND ("payload.permission_type" IN (
              'PERMISSION_TYPE_ANDROID_SETUP',
              'PERMISSION_TYPE_LOCATION',
              'PERMISSION_TYPE_PHONE',
              'PERMISSION_TYPE_CONTACT',
              'PERMISSION_TYPE_MICROPHONE'
            ))
            AND (NULLIF(adjust_id, '') IS NOT NULL)
            AND (user_id IS NOT NULL)
            AND (created_at BETWEEN installed_at AND installed_at + INTERVAL '24 hours')

        GROUP BY 1, 2, 3
    )
    SELECT
        client_type,
        adjust_id,
        MAX(CASE WHEN permission_type = 'PERMISSION_TYPE_ANDROID_SETUP' THEN permission_state END) AS permission_android_setup_state,
        MAX(CASE WHEN permission_type = 'PERMISSION_TYPE_LOCATION' THEN permission_state END) AS permission_location_state,
        MAX(CASE WHEN permission_type = 'PERMISSION_TYPE_PHONE' THEN permission_state END) AS permission_phone_state,
        MAX(CASE WHEN permission_type = 'PERMISSION_TYPE_CONTACT' THEN permission_state END) AS permission_contact_state,
        MAX(CASE WHEN permission_type = 'PERMISSION_TYPE_MICROPHONE' THEN permission_state END) AS permission_microphone_state
    FROM perms
    GROUP BY 1, 2
),

user_device AS (
    SELECT DISTINCT u.client_type, date_utc, adjust_id, user_id_hex, installed_at
    FROM {{ source('dau', 'user_device_master') }} u
    JOIN base a ON (u.client_type = a.client_type) AND (u.adid = a.adjust_id)
    JOIN {{ ref('analytics_users') }} USING (username)
    WHERE

    {% if is_incremental() %}
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '31 DAYS' AND {{ var('ds') }}::DATE)
    {% else %}
        (date_utc BETWEEN '2020-08-01'::DATE AND {{ var('ds') }}::DATE)
    {% endif %}
),

calls AS (
    SELECT
        client_type,
        adjust_id,
        SUM(CASE
            WHEN calls.created_at BETWEEN installed_at AND installed_at + INTERVAL '24 hours' AND
                "payload.call_direction" = 'CALL_DIRECTION_OUTBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d1_out_call_duration,
        SUM(CASE
            WHEN calls.created_at BETWEEN installed_at AND installed_at + INTERVAL '24 hours' AND
                "payload.call_direction" = 'CALL_DIRECTION_INBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d1_in_call_duration,
        SUM(CASE
            WHEN calls.created_at BETWEEN installed_at AND installed_at + INTERVAL '72 hours' AND
                "payload.call_direction" = 'CALL_DIRECTION_OUTBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d3_out_call_duration,
        SUM(CASE
            WHEN calls.created_at BETWEEN installed_at AND installed_at + INTERVAL '72 hours' AND
                "payload.call_direction" = 'CALL_DIRECTION_INBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d3_in_call_duration,
        SUM(CASE
            WHEN "payload.call_direction" = 'CALL_DIRECTION_OUTBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d5_out_call_duration,
        SUM(CASE
            WHEN "payload.call_direction" = 'CALL_DIRECTION_INBOUND' THEN "payload.call_duration.seconds" ELSE 0
        END) AS d5_in_call_duration
    FROM user_device
    JOIN {{ source('party_planner_realtime', 'call_completed') }} calls ON

    {% if is_incremental() %}
        (calls.created_at >= {{ var('ds') }}::DATE - INTERVAL '31 DAYS')
    {% else %}
        (calls.created_at BETWEEN '2020-08-01'::DATE AND {{ var('ds') }}::DATE)
    {% endif %}

        AND (calls.user_id_hex = user_device.user_id_hex)
        AND (calls.created_at::DATE = user_device.date_utc)
        AND (calls.user_id IS NOT NULL)
        AND (calls.created_at BETWEEN installed_at AND installed_at + INTERVAL '120 hours')
    GROUP BY 1, 2
),

messages AS (
    SELECT
        client_type,
        adjust_id,
        SUM(CASE
            WHEN messages.created_at BETWEEN installed_at AND installed_at + INTERVAL '24 hours' AND
                "payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' THEN 1 ELSE 0
        END) AS d1_out_msg,
        SUM(CASE
            WHEN messages.created_at BETWEEN installed_at AND installed_at + INTERVAL '24 hours' AND
                "payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' THEN 1 ELSE 0
        END) AS d1_in_msg,
        SUM(CASE
            WHEN messages.created_at BETWEEN installed_at AND installed_at + INTERVAL '72 hours' AND
                "payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' THEN 1 ELSE 0
        END) AS d3_out_msg,
        SUM(CASE
            WHEN messages.created_at BETWEEN installed_at AND installed_at + INTERVAL '72 hours' AND
                "payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' THEN 1 ELSE 0
        END) AS d3_in_msg,
        SUM(CASE
            WHEN "payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' THEN 1 ELSE 0
        END) AS d5_out_msg,
        SUM(CASE
            WHEN "payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' THEN 1 ELSE 0
        END) AS d5_in_msg
    FROM user_device
    JOIN {{ source('party_planner_realtime', 'message_delivered') }} messages ON

    {% if is_incremental() %}
        (messages.created_at >= {{ var('ds') }}::DATE - INTERVAL '31 DAYS')
    {% else %}
        (messages.created_at BETWEEN '2020-08-01'::DATE AND {{ var('ds') }}::DATE)
    {% endif %}

        AND (messages.user_id_hex = user_device.user_id_hex)
        AND (messages.created_at::DATE = user_device.date_utc)
        AND (messages.user_id IS NOT NULL)
        AND (messages.created_at BETWEEN installed_at AND installed_at + INTERVAL '120 hours')
    GROUP BY 1, 2
)

SELECT *
FROM base
LEFT JOIN permission_states using (client_type, adjust_id)
LEFT JOIN messages using (client_type, adjust_id)
LEFT JOIN calls using (client_type, adjust_id)
