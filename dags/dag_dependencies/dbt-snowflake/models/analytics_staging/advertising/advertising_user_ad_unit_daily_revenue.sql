{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

/*
impressions assigned to specific user names and line_item_ids/ad_units
 */
WITH total_impressions AS (
    SELECT date_utc,
           username,
           client_type,
           ad_unit,
           SUM(NVL(impressions, 0)) AS user_impressions
    FROM {{ ref('advertising_line_item_ad_unit_user_daily_impressions') }}
    WHERE NVL(ad_unit, '') <> ''
      AND date_utc >='2020-01-01'

    {% if is_incremental() %}

      AND date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }})

    {% endif %}

    GROUP BY date_utc,
             client_type,
             username,
             ad_unit
),
/*
revenue assigned to specific line_item_ids/ad_units
*/
overall_revenue AS (
    SELECT  date_utc,
            client_type,
            ad_unit,
            SUM(NVL(revenue, 0))    AS sum_revenue
    FROM {{ ref('advertising_line_item_ad_unit_daily_revenue') }}
    WHERE NVL(ad_unit, '') <> ''
      AND date_utc >='2020-01-01'

    {% if is_incremental() %}

      AND date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '1 week'

    {% endif %}

    GROUP BY date_utc,
             client_type,
             ad_unit
),
/*
matching the impressions and revenue from above two tables by joining on the line_item_id
*/
matched_user_revenue AS (
    SELECT date_utc,
           username,
           client_type,
           ad_unit,
           user_impressions,
           NVL(sum_revenue * RATIO_TO_REPORT(user_impressions) OVER (PARTITION BY date_utc, ad_unit), 0) AS user_revenue
    FROM total_impressions
    JOIN overall_revenue USING (    date_utc,
                                    client_type,
                                    ad_unit)
),
/*
putting together the matched impressions and revenue by user
*/
daily_revenue AS (
    SELECT date_utc,
           username,
           client_type,
           ad_unit,
           SUM(NVL(user_impressions, 0)) AS ad_impressions,
           SUM(NVL(user_revenue, 0))     AS ad_revenue
    FROM matched_user_revenue
    GROUP BY date_utc,
             client_type,
             username,
             ad_unit
)
SELECT  date_utc,
        username,
        client_type,
        ad_unit,
        ad_impressions,
        ad_revenue
FROM daily_revenue