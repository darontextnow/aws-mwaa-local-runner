{{
    config(
        tags=['daily'],
        alias='registrations',
        materialized='incremental',
        unique_key='USER_ID_HEX',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH adjust_regs AS (

    SELECT USERNAME                                                                 AS USERNAME,
           COUNTRY_CODE                                                             AS COUNTRY_CODE
    FROM {{ ref('adjust_registrations') }}
    WHERE TRIM(NVL(USERNAME, '')) <> ''

    {% if is_incremental() %}
      AND CREATED_AT >= (SELECT MAX(REGISTERED_AT) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY USERNAME ORDER BY CREATED_AT) = 1

),

pp_regs AS (

    SELECT "client_details.client_data.user_data.username"                          AS USERNAME,
           NULLIF("client_details.client_data.tz_code", '')                         AS TZ_CODE,
           UPPER(NULLIF("client_details.client_data.country_code", ''))             AS COUNTRY_CODE
    FROM {{ source('party_planner_realtime', 'registrations') }} r
    JOIN {{ source('support', 'timezones') }} t
      ON NVL(TRIM(r."client_details.client_data.tz_code"), '') = t.TZ_CODE
    WHERE INSTANCE_ID LIKE 'TN_SERVER%'
      AND NVL(TRIM(USER_ID_HEX), '') <> ''
      AND "payload.result" = 'RESULT_OK'

    {% if is_incremental() %}
      AND CREATED_AT >= (SELECT MAX(REGISTERED_AT) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}

    QUALIFY ROW_NUMBER() OVER
        (PARTITION BY "client_details.client_data.user_data.username" ORDER BY CREATED_AT) = 1

),

legacy_regs AS (

    SELECT USERNAME                                                                 AS USERNAME,
           CREATED_AT                                                               AS CREATED_AT,
           COUNTRY_CODE                                                             AS COUNTRY_CODE
    FROM {{ source('firehose', 'registrations') }} r
    WHERE HTTP_RESPONSE_STATUS <= 299
      AND USERNAME NOT IN (SELECT USERNAME FROM {{ this }})

    {% if is_incremental() %}
      AND CREATED_AT >= (SELECT MAX(REGISTERED_AT) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY USERNAME ORDER BY CREATED_AT) = 1

)

{% if is_incremental() %}

SELECT COALESCE(o.REGISTERED_AT, u.CREATED_AT, c.CREATED_AT)                        AS REGISTERED_AT,
       IFF(b.TZ_CODE IS NOT NULL,
           CONVERT_TIMEZONE(b.TZ_CODE, REGISTERED_AT),
           NULL)                                                                    AS LOCAL_REGISTERED_AT,
       u.USER_ID_HEX                                                                AS USER_ID_HEX,
       u.USERNAME                                                                   AS USERNAME,
       REGEXP_REPLACE(NVL(UPPER(i.PROVIDER), 'EMAIL'), '_SIGNIN')                   AS REGISTRATION_PROVIDER,
       COALESCE(o.COUNTRY_CODE, a.COUNTRY_CODE, b.COUNTRY_CODE, c.COUNTRY_CODE)     AS COUNTRY_CODE
FROM {{ source('core', 'users') }} u
LEFT JOIN {{ source('core', 'identities') }} i ON u.USERNAME = i.USERNAME
LEFT JOIN adjust_regs a ON u.USERNAME = a.USERNAME
LEFT JOIN pp_regs b ON u.USERNAME = b.USERNAME
LEFT JOIN legacy_regs c ON u.USERNAME = c.USERNAME
LEFT JOIN {{ this }} o ON u.USER_ID_HEX = o.USER_ID_HEX
WHERE COALESCE(o.REGISTERED_AT, u.CREATED_AT, c.CREATED_AT) IS NOT NULL
  AND COALESCE(o.REGISTERED_AT, u.CREATED_AT, c.CREATED_AT) >= (SELECT MAX(REGISTERED_AT) - INTERVAL '14 days' FROM {{ this }})

{% else %}

SELECT COALESCE(u.CREATED_AT, c.CREATED_AT)                                         AS REGISTERED_AT,
       IFF(b.TZ_CODE IS NOT NULL,
           CONVERT_TIMEZONE(b.TZ_CODE, REGISTERED_AT),
           NULL)                                                                    AS LOCAL_REGISTERED_AT,
       u.USER_ID_HEX                                                                AS USER_ID_HEX,
       u.USERNAME                                                                   AS USERNAME,
       REGEXP_REPLACE(NVL(UPPER(i.PROVIDER), 'EMAIL'), '_SIGNIN')                   AS REGISTRATION_PROVIDER,
       COALESCE(a.COUNTRY_CODE, b.COUNTRY_CODE, c.COUNTRY_CODE)                     AS COUNTRY_CODE
FROM {{ source('core', 'users') }} u
LEFT JOIN {{ source('core', 'identities') }} i ON u.USERNAME = i.USERNAME
LEFT JOIN adjust_regs a ON u.USERNAME = a.USERNAME
LEFT JOIN pp_regs b ON u.USERNAME = b.USERNAME
LEFT JOIN legacy_regs c ON u.USERNAME = c.USERNAME
WHERE REGISTERED_AT IS NOT NULL

{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY u.USER_ID_HEX ORDER BY REGISTERED_AT) = 1