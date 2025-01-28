{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}
/*
rolling up impressions to user/line_item_id level
Segregated in-house impressions from user impressions for matched and umatched revenue
 */

WITH total_impressions AS (
    SELECT 
        date_utc,
        username,
        client_type,
        line_item_id,
        SUM(NVL(CASE WHEN ad_unit = 'IN-HOUSE' THEN impressions END, 0)) AS in_house_impressions,
        SUM(NVL(CASE WHEN ad_unit <> 'IN-HOUSE' THEN impressions END, 0)) AS user_impressions
    FROM {{ ref('advertising_line_item_ad_unit_user_daily_impressions') }}
    WHERE
        (date_utc >= '2020-01-01'::DATE)

    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '30 days')
    {% endif %}

    {% if target.name == 'dev' %}
        AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}
        AND (NVL(line_item_id, '') <> '')

    GROUP BY 1, 2, 3, 4
            
),
/*
rolling the revenue to line_item_id level
*/
overall_revenue AS (
    SELECT
        date_utc,
        client_type,
        line_item_id,
        SUM(NVL(revenue, 0)) AS sum_revenue
    FROM {{ ref('advertising_line_item_ad_unit_daily_revenue') }}
    WHERE
        (NVL(line_item_id, '') <> '')
        AND (date_utc >= '2020-01-01'::DATE)
    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '30 days')
    {% endif %}
    {% if target.name == 'dev' %}
        AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}
    GROUP BY 1, 2, 3
),
/*
matching specifically on the line_item_id level and distributing by user
This would capture impressions and  revenue for non in house ads only
*/
matched_user_revenue AS (
    SELECT
        date_utc,
        username,
        client_type,
        line_item_id,
        in_house_impressions,
        user_impressions,
        NVL(sum_revenue * RATIO_TO_REPORT(user_impressions) OVER (PARTITION BY date_utc, line_item_id), 0)  AS user_revenue
    FROM total_impressions
    JOIN overall_revenue USING (date_utc, client_type, line_item_id)

),
matched_line_items AS (
    SELECT DISTINCT date_utc, line_item_id
    FROM matched_user_revenue
),
/*
getting the specific impressions not accounted for in the above merge
*/
unmatched_impressions AS (
    SELECT
        date_utc,
        username,
        client_type,
        ad_unit,
        SUM(NVL(CASE WHEN ad_unit='IN-HOUSE' THEN impressions END, 0)) AS in_house_impressions,
        SUM(NVL(CASE WHEN ad_unit<>'IN-HOUSE' THEN impressions END, 0)) AS user_impressions
    FROM {{ ref('advertising_line_item_ad_unit_user_daily_impressions') }}
    LEFT JOIN matched_line_items USING (date_utc, line_item_id)
    WHERE
        (date_utc >='2020-01-01'::DATE)
    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '30 days')
    {% endif %}
    {% if target.name == 'dev' %}
        AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}
        AND (matched_line_items.line_item_id IS NULL)
        AND (NVL(username, '') <> '')
        AND (NVL(ad_unit, '') <> '')
        AND (ad_unit NOT IN ('OTHER'))
        AND (client_type NOT IN ('OTHER'))
    GROUP BY 1, 2, 3, 4
),
/*
rolling up the revenue by ad unit which is not accounted for in the matched table
*/
unmatched_revenue AS (
    SELECT
        date_utc,
        client_type,
        ad_unit,
        SUM(NVL(revenue, 0)) AS sum_revenue
    FROM {{ ref('advertising_line_item_ad_unit_daily_revenue') }}
    LEFT JOIN matched_line_items USING (date_utc, line_item_id)
    WHERE
        (date_utc >= '2020-01-01'::DATE)
    {% if is_incremental() %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '30 days')
    {% endif %}
    {% if target.name == 'dev' %}
        AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}
        AND (matched_line_items.line_item_id IS NULL)
        AND (NVL(ad_unit, '') <> '')
        AND (ad_unit NOT IN ('OTHER'))
        AND (client_type NOT IN ('OTHER'))
    GROUP BY 1, 2, 3
),
/*
putting the above two tables together
This would capture impressions for non in house as well as in house ads but revenue only for non in house
*/
additional_estimates AS (
    SELECT
        date_utc,
        username,
        client_type,
        ad_unit,
        in_house_impressions,
        user_impressions ,
        NVL(sum_revenue * RATIO_TO_REPORT(user_impressions) OVER (PARTITION BY date_utc, ad_unit), 0) AS revenue_adjustments
    FROM unmatched_impressions   --Left outer join to get all the usernames and impressions which does not have matching records from adops
    LEFT OUTER JOIN unmatched_revenue USING (date_utc, client_type, ad_unit)
),
matched_table AS (
    SELECT
        date_utc,
        username,
        client_type,
        SUM(NVL(in_house_impressions,0)) AS matched_in_house_impressions,
        SUM(NVL(user_impressions, 0)) AS matched_impressions,
        SUM(NVL(user_revenue, 0)) AS matched_revenue
    FROM matched_user_revenue
    GROUP BY 1, 2, 3
),
unmatched_table AS (
    SELECT
        date_utc,
        username,
        client_type,
        SUM(NVL(user_impressions, 0)) AS unmatched_impressions,
        SUM(NVL(in_house_impressions, 0)) AS unmatched_in_house_impressions,
        SUM(NVL(revenue_adjustments, 0)) AS revenue_adjustments
    FROM additional_estimates
    GROUP BY 1, 2, 3
),
user_daily_revenue AS (
     SELECT
        date_utc,
        username,
        {{ normalized_client_type('client_type') }} AS client_type,
        SUM(NVL(matched_impressions, 0)) AS matched_impressions,
        SUM(NVL(matched_revenue, 0)) AS matched_revenue,
        SUM(NVL(unmatched_impressions, 0)) AS unmatched_impressions,
        SUM(NVL(unmatched_in_house_impressions, 0))+SUM(NVL(matched_in_house_impressions,0)) AS in_house_impressions,
        SUM(NVL(revenue_adjustments, 0)) AS revenue_adjustments,
        SUM(NVL(matched_impressions, 0)) + SUM(NVL(unmatched_impressions, 0)) AS adjusted_impressions
    FROM matched_table
    FULL OUTER JOIN unmatched_table USING (date_utc, username, client_type)
    GROUP BY 1, 2, 3
),
/*
extracting the sum of matched and unmatched revenue from user_daily_revenue and figuring out the left-over amount for each date
*/
adjusted_revenue_table AS (
    SELECT date_utc, SUM(NVL(matched_revenue, 0)) + SUM(NVL(revenue_adjustments, 0)) AS sum_revenue
    FROM user_daily_revenue
    GROUP BY date_utc
),
/*
taking the leftover revenue which cannot be attributed to anything specific
*/
unattributable_revenue AS (
    SELECT
        date_utc,
        adops.revenue AS total_ad_revenue,
        adjusted_revenue_table.sum_revenue AS total_adjusted_revenue,
        GREATEST(total_ad_revenue - total_adjusted_revenue, 0) AS unattributable_revenue
    FROM adjusted_revenue_table
    JOIN (
        SELECT "DATE" AS date_utc, SUM(NVL(revenue, 0)) AS revenue
        FROM {{ ref('report') }}
        WHERE (date_utc >= '2020-01-01'::DATE)

      {% if is_incremental() %}
            AND ("DATE" >= {{ var('ds') }}::DATE - INTERVAL '30 days')
      {% endif %}

      {% if target.name == 'dev' %}
            AND ("DATE" >= CURRENT_DATE - INTERVAL '1 week')
      {% endif %}

      GROUP BY 1
    ) AS adops USING (date_utc)
    WHERE (date_utc >= '2020-01-01'::DATE)
)
/*Distribute the unattributable revenue based on the count of  impressions excluding in-house */
SELECT 
    user_daily_revenue.*,
    NVL(unattributable_revenue.unattributable_revenue, 0) *CAST(adjusted_impressions/(SUM(adjusted_impressions) OVER (PARTITION BY date_utc)::double) AS numeric(35,20)) AS avg_truly_unattributable_revenue,
    NVL((matched_revenue + revenue_adjustments + avg_truly_unattributable_revenue), 0) AS adjusted_revenue
FROM user_daily_revenue
LEFT JOIN unattributable_revenue USING (date_utc)
