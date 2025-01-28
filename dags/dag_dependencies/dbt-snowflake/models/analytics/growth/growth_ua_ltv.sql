{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='cohort_day',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- Sourcing data from ua_cost_attribution_adjustment which includes adjust cost data and the manual override data.
WITH ua_spend AS (
    SELECT
        c.cohort_utc AS cohort_utc,
        DECODE(c.country_code, 'US', 'US', 'CA', 'CA', 'Non-US/CA') AS geo,
        c.client_type,
        c.network_name AS network_name,
        COALESCE(c.campaign_name, '') AS campaign_name,
        SUM(c.cost) AS ua_cost
    FROM  {{ ref('ua_cost_attribution_adjustment') }} AS c
    WHERE c.cohort_utc < '2023-09-01'

    {% if is_incremental() %}
        AND c.cohort_utc >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(c.cost) > 0 

    UNION ALL SELECT
        c.date AS cohort_utc,
        DECODE(c.country_field, 'USA', 'US', 'CAN', 'CA', 'Non-US/CA') AS geo,
        CASE WHEN c.site_public_id = 'com.enflick.android.TextNow' THEN 'TN_ANDROID'
            WHEN c.site_public_id = '314716233' THEN 'TN_IOS_FREE'
            END AS client_type,
        COALESCE(mp.adjust_network_name,c.data_connector_source_name) AS network_name,
        COALESCE(c.adn_campaign_name, '') AS campaign_name,
        SUM(c.adn_cost) AS ua_cost
    FROM  {{ source('singular', 'marketing_data') }} AS c
    LEFT OUTER JOIN  {{ ref('ua_singular_to_adjust_network_mapping') }} AS mp ON
        c.data_connector_source_name = mp.singular_network_name
    WHERE
        c.date < CURRENT_DATE
        AND c.date >= '2023-09-01'

    {% if is_incremental() %}
        AND c.date >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(c.adn_cost) > 0
),


margins AS (
    SELECT
        year_month,
        'TN_ANDROID' AS client_type,
        'US' AS geo,
        margin_android_US_365 AS margin_365,
        margin_android_US_730 AS margin_730
    FROM {{ ref('ua_margin_per_active_day_by_geo') }}

    UNION ALL SELECT
        year_month,
        'TN_ANDROID' AS client_type,
        'CA' AS geo,
        margin_android_CA_365 AS margin_365,
        margin_android_CA_730 AS margin_730
    FROM {{ ref('ua_margin_per_active_day_by_geo') }}

    UNION ALL SELECT
        year_month,
        'TN_IOS_FREE' AS client_type,
        'US' AS geo,
        margin_ios_US_365 AS margin_365,
        margin_ios_US_730 AS margin_730
    FROM {{ ref('ua_margin_per_active_day_by_geo') }}

    UNION ALL SELECT
        year_month,
        'TN_IOS_FREE' AS client_type,
        'CA' AS geo,
        margin_ios_CA_365 AS margin_365,
        margin_ios_CA_730 AS margin_730
    FROM {{ ref('ua_margin_per_active_day_by_geo') }}
),

active_days_prediction AS (
    SELECT
        p.date_utc AS cohort_utc,
        DECODE(p.country_code, 'US', 'US', 'CA', 'CA', 'Non-US/CA') AS geo,
        p.client_type,
        COALESCE(network_name, '') AS network_name,
        COALESCE(campaign_name, '') AS campaign_name,
        SUM(CASE WHEN install_number_bin = 0
            THEN installs * predicted_dau_365 ELSE 0 END) AS new_installs_365_active_days,
        SUM(CASE WHEN install_number_bin > 0
            THEN installs * predicted_dau_365 ELSE 0 END) AS reinstalls_365_active_days,
        SUM(installs * predicted_dau_365) AS overall_365_active_days,
        SUM(installs * predicted_dau_365 * margin_365) AS overall_365_ltv,
        SUM(CASE WHEN install_number_bin = 0
            THEN installs * predicted_dau_730 ELSE 0 END) AS new_installs_730_active_days,
        SUM(CASE WHEN install_number_bin > 0
            THEN installs * predicted_dau_730 ELSE 0 END) AS reinstalls_730_active_days,
        SUM(installs * predicted_dau_730) AS overall_730_active_days,
        SUM(installs * predicted_dau_730 * margin_730) AS overall_730_ltv
    FROM {{ source('analytics', 'ltv_active_days_prediction') }} p
    LEFT JOIN margins on
        DATE_TRUNC('month', p.date_utc) = DATE_TRUNC('month', margins.year_month)
        AND margins.client_type = p.client_type
        AND margins.geo = p.country_code
    WHERE p.date_utc < CURRENT_DATE() - INTERVAL '7 days'

    {% if is_incremental() %}
        AND p.date_utc >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),

user_installs AS (
    SELECT
        i.adjust_id,
        i.installed_at,
        i.installed_at::DATE AS cohort_utc,
        DECODE(i.country_code, 'US', 'US', 'CA', 'CA', 'Non-US/CA') AS geo,
        i.client_type,
        t.network_name,
        COALESCE(t.campaign_name, '') AS campaign_name
    FROM {{ ref('installs') }} AS i
    JOIN {{ ref('adjust_trackers') }} AS t USING (client_type, tracker)
    WHERE i.installed_at < CURRENT_DATE

    {% if is_incremental() %}
        AND i.installed_at >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}
),

user_generation AS (
    SELECT
        i.cohort_utc,
        i.geo,
        i.client_type,
        i.network_name,
        i.campaign_name,
        COUNT(i.adjust_id) AS installs,
        SUM(d.new_user_install) AS new_user_installs,
        SUM(d.old_user_install) AS old_user_installs,
        SUM(e.active_on_day_3) AS d3_retained,
        SUM(d.week2_retained) AS w2_retained,
        SUM(d.w2r_prob) AS w2r_prob,
        SUM(d.dau_generation) AS dau_generation
    FROM user_installs i
    LEFT JOIN {{ ref('growth_installs_dau_generation') }} AS d USING (client_type, adjust_id, installed_at)
    LEFT JOIN {{ ref('ua_post_install_events') }} AS e USING (client_type, adjust_id, installed_at)
    WHERE d.installed_at < CURRENT_DATE

    {% if is_incremental() %}
        AND d.installed_at >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),

new_user_active_days AS (
    SELECT
        i.cohort_utc,
        i.geo,
        i.client_type,
        i.network_name,
        i.campaign_name,
        SUM(CASE WHEN a.day_from_cohort = 0 THEN dau ELSE 0 END) AS total_d0_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 1 AND 7 THEN dau ELSE 0 END) AS total_d1_d7_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 8 AND 14 THEN dau ELSE 0 END) AS total_d8_d14_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 15 AND 21 THEN dau ELSE 0 END) AS total_d15_d21_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 22 AND 28 THEN dau ELSE 0 END) AS total_d22_d28_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 29 AND 30 THEN dau ELSE 0 END) AS total_d29_d30_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 31 AND 90 THEN dau ELSE 0 END) AS total_m02_m03_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 91 AND 180 THEN dau ELSE 0 END) AS total_m04_m06_dau,
        SUM(CASE WHEN a.day_from_cohort BETWEEN 181 AND 365 THEN dau ELSE 0 END) AS total_m07_m12_dau
    FROM user_installs i
    JOIN {{ source('dau', 'set_cohort') }} AS d ON
        d.adid = i.adjust_id
        AND d.client_type = i.client_type
        AND d.cohort_utc = i.cohort_utc
    JOIN {{ ref('dau_user_set_active_days') }} AS a ON
        a.user_set_id = d.set_uuid
        AND a.client_type = d.client_type
        AND d.cohort_utc = a.cohort_utc
    WHERE a.date_utc < CURRENT_DATE
        AND a.user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }})
        AND a.day_from_cohort BETWEEN 0 AND 365

    {% if is_incremental() %}
        AND a.date_utc >= (SELECT MAX(cohort_day) - INTERVAL '370 days' FROM {{ this }})
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),

cte_ltv_installs_spend AS (
    SELECT
        cohort_utc AS cohort_day,
        DATE_TRUNC('week', cohort_utc) AS cohort_week,
        DATE_TRUNC('month', cohort_utc) AS cohort_month,
        DATE_TRUNC('year', cohort_utc) AS cohort_year,
        geo,
        client_type,
        network_name,
        campaign_name,
        COALESCE(c.ua_cost, 0) AS ua_cost,
        COALESCE(u.installs, 0) AS installs,
        COALESCE(u.new_user_installs, 0) AS new_user_installs,
        COALESCE(u.old_user_installs, 0) AS old_user_installs,
        COALESCE(u.d3_retained, 0) AS d3_retained,
        u.w2_retained AS w2_retained,
        u.w2r_prob AS synthetic_w2r,
        u.dau_generation,
        COALESCE(d.total_d0_dau, 0) AS new_users_d0_active_days,
        COALESCE(d.total_d1_d7_dau, 0) AS new_users_d1_d7_active_days,
        COALESCE(d.total_d8_d14_dau, 0) AS new_users_d8_d14_active_days,
        COALESCE(d.total_d15_d21_dau, 0) AS new_users_d15_d21_active_days,
        COALESCE(d.total_d22_d28_dau, 0) AS new_users_d22_d28_active_days,
        COALESCE(d.total_d29_d30_dau, 0) AS new_users_d29_d30_active_days,
        COALESCE(d.total_m02_m03_dau, 0) AS new_users_m02_m03_active_days,
        COALESCE(d.total_m04_m06_dau, 0) AS new_users_m04_m06_active_days,
        COALESCE(d.total_m07_m12_dau, 0) AS new_users_m07_m12_active_days,
        a.overall_365_active_days AS active_days_365,
        a.overall_730_active_days AS active_days_730,
        a.new_installs_365_active_days AS active_days_365_new_installs,
        a.reinstalls_365_active_days AS active_days_365_reinstalls,
        a.new_installs_730_active_days AS active_days_730_new_installs,
        a.reinstalls_730_active_days AS active_days_730_reinstalls,
        a.overall_365_ltv AS ltv_365,
        a.overall_730_ltv AS ltv_730
    FROM user_generation AS u
    FULL OUTER JOIN active_days_prediction a USING (cohort_utc, geo, client_type, network_name, campaign_name)
    FULL OUTER JOIN ua_spend AS c USING (cohort_utc, geo, client_type, network_name, campaign_name)
    FULL OUTER JOIN new_user_active_days AS d USING (cohort_utc, geo, client_type, network_name, campaign_name)
)

--Add the network_name root column to the final dataset to facilitate mapping of all networks belonging to the same partner.
SELECT
    cohort_day,
    cohort_week,
    cohort_month,
    cohort_year,
    geo,
    client_type,
    cpi.network_name,
    campaign_name,
    ua_cost,
    installs,
    new_user_installs,
    old_user_installs,
    d3_retained,
    w2_retained,
    synthetic_w2r,
    dau_generation,
    new_users_d0_active_days,
    new_users_d1_d7_active_days,
    new_users_d8_d14_active_days,
    new_users_d15_d21_active_days,
    new_users_d22_d28_active_days,
    new_users_d29_d30_active_days,
    new_users_m02_m03_active_days,
    new_users_m04_m06_active_days,
    new_users_m07_m12_active_days,
    active_days_365,
    active_days_730,
    active_days_365_new_installs,
    active_days_365_reinstalls,
    active_days_730_new_installs,
    active_days_730_reinstalls,
    ltv_365,
    ltv_730,
    map.network_name_root,
    CASE
        WHEN REGEXP_LIKE(campaign_name, '(.+)\\s\\([0-9]{4}[0-9]+\\)')
        THEN TRIM(REGEXP_SUBSTR(campaign_name, '(.+)\\s\\([0-9]+\\)', 1, 1, 'e', 1))
        WHEN cmap.actual_campaign_name IS NOT NULL THEN cmap.display_campaign_name
        ELSE cpi.campaign_name
    END AS updated_campaign_name
FROM cte_ltv_installs_spend cpi
LEFT OUTER JOIN {{ ref('network_campaigns_mapping') }} map ON cpi.network_name = map.network_name
LEFT OUTER JOIN {{ ref('campaign_mapping') }}  cmap ON cpi.campaign_name = cmap.actual_campaign_name
