{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}


WITH INSTALL_CTE AS (
    SELECT DISTINCT adjust_id AS installs,
        client_type,
        COUNTRY_CODE,
        installed_at::DATE AS date_utc,
        is_organic
    FROM {{ ref('installs') }}
    WHERE
        date_utc >= '2021-01-01'
        AND UPPER(CLIENT_TYPE) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID')
        AND is_untrusted = FALSE
        AND UPPER(country_code) IN ('US', 'CA')
)
SELECT date_utc,
    CASE
        WHEN UPPER(CLIENT_TYPE) IN ('TN_ANDROID', '2L_ANDROID') THEN 'ANDROID (INCL 2L)'
        ELSE CLIENT_TYPE
    END AS CLIENT_TYPE,
    COUNTRY_CODE,
    COUNT(installs) AS installs,
    COUNT(
        CASE
            WHEN IS_ORGANIC = 'TRUE' THEN DATE_UTC
        END
    ) AS Organic,
    COUNT(
        CASE
            WHEN IS_ORGANIC = 'FALSE' THEN DATE_UTC
        END
    ) AS Paid
FROM INSTALL_CTE
GROUP BY 1, 2, 3
ORDER BY 1 DESC