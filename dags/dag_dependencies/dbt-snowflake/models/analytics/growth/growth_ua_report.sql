{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_day'
    )
}}

/*Sourcing data from ua_cost_attribution_adjustment which includes adjust cost data and the manual override data.*/
WITH ua_spend AS (
    SELECT
        c.cohort_utc,
        DECODE(c.country_code, 'US', 'US', 'CA', 'CA', 'Non-US/CA') AS geo,
        c.client_type,
        c.network_name,
        COALESCE(c.campaign_name, '') AS campaign_name,
        COALESCE(c.adgroup_name, '') AS adgroup_name,
        SUM(c.cost) AS ua_cost
    FROM {{ ref('ua_cost_attribution_adjustment') }} AS c
    WHERE c.cohort_utc < '2023-09-01'::DATE
    {% if is_incremental() %}
        AND c.cohort_utc >= {{ var('ds') }}::DATE - INTERVAL '35 days'
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING SUM(c.cost) > 0
    UNION ALL SELECT
        c.date AS cohort_utc,
        DECODE(c.country_field, 'USA', 'US', 'CAN', 'CA', 'Non-US/CA') AS geo,
        CASE WHEN c.site_public_id = 'com.enflick.android.TextNow' THEN 'TN_ANDROID'
            WHEN c.site_public_id = '314716233' THEN 'TN_IOS_FREE'
            END AS client_type,
        COALESCE(mp.adjust_network_name,c.data_connector_source_name) AS network_name,
        COALESCE(c.adn_campaign_name, '') AS campaign_name,
        COALESCE(c.adn_sub_campaign_name, '') AS adgroup_name,
        SUM(c.adn_cost) AS ua_cost
    FROM {{ source('singular', 'marketing_data') }} AS c
    LEFT OUTER JOIN {{ ref('ua_singular_to_adjust_network_mapping') }} AS mp ON
        c.data_connector_source_name = mp.singular_network_name
    WHERE
        c.date < {{ var('ds') }}::DATE + INTERVAL '1 day'
        AND c.date >= '2023-09-01'::DATE
    {% if is_incremental() %}
        AND c.date >= {{ var('ds') }}::DATE - INTERVAL '35 days'
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING SUM(c.adn_cost) > 0
),
ua_ltv AS (
    SELECT
        app_id,
        adid AS adjust_id,
        SUM(active * revenue) AS ltv365
    FROM ext.ltv_ua_install
    WHERE
        day_from_install <= 365
        AND cohort < {{ var('ds') }}::DATE + INTERVAL '1 day'
    {% if is_incremental() %}
        AND cohort >= {{ var('ds') }}::DATE - INTERVAL '35 days'
    {% endif %}
    GROUP BY 1, 2
),

user_generation AS (
    SELECT
        i.installed_at::DATE AS cohort_utc,
        DECODE(i.country_code, 'US', 'US', 'CA', 'CA', 'Non-US/CA') AS geo,
        i.client_type,
        'adjust_installs' AS installation_type,
        t.network_name,
        COALESCE(t.campaign_name, '') AS campaign_name,
        COALESCE(t.adgroup_name, '') AS adgroup_name,
        COUNT(i.adjust_id) AS installs,
        SUM(c.outbound_call) AS outbound_calls,
        SUM(c.lp_sessions) AS lp_sessions,
        SUM(c.combined_event) AS combined_event,
        SUM(d.new_user_install) AS new_user_installs,
        SUM(d.old_user_install) AS old_user_installs,
        SUM(e.active_on_day_3) AS d3_retained,
        SUM(d.week2_retained) AS w2_retained,
        SUM(d.w2r_prob) AS w2r_prob,
        SUM(d.dau_generation) AS dau_generation,
        SUM(CASE WHEN e.d3_out_msg > 0 OR e.d3_out_call_duration > 0 THEN 1 ELSE 0 END) AS first_comm,
        SUM(v.ltv365) AS ltv365
    FROM {{ ref('installs') }} AS i
    JOIN {{ ref('adjust_trackers') }} AS t USING (client_type, tracker)
    LEFT JOIN {{ ref('growth_installs_dau_generation') }} AS d USING (client_type, adjust_id, installed_at)
    LEFT JOIN {{ ref('ua_post_install_events') }} AS e USING (client_type, adjust_id, installed_at)
    LEFT JOIN  {{ ref('ua_early_mover_event') }} AS c USING (client_type, adjust_id, installed_at)
    LEFT JOIN ua_ltv AS v USING (app_id, adjust_id)
    WHERE i.installed_at < {{ var('ds') }}::DATE + INTERVAL '1 day'
    {% if is_incremental() %}
        AND i.installed_at >= {{ var('ds') }}::DATE - INTERVAL '35 days'
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    --installs from skad network for ios 14.1
    UNION ALL SELECT
        sk.installed_at::DATE AS cohort_utc,
        DECODE(sk.country, 'us', 'US', 'ca', 'CA', 'US') AS geo,
        sk.client_type,
        'skad_installs' AS installation_type,
        COALESCE(t.network_name,sk.partner_name) AS network_name,
        COALESCE(sk.campaign_name, '') AS campaign_name,
        COALESCE(sk.adgroup_name, '') AS adgroup_name,
        COUNT(sk.transaction_id) AS installs,
        0 AS outbound_calls,
        0 AS lp_sessions,
        0 AS combined_event,
        0 AS new_user_installs,
        0 AS old_user_installs,
        0 AS d3_retained,
        0 AS w2_retained,
        0 AS w2r_prob,
        0 AS dau_generation,
        0 AS first_comm,
        0 AS ltv365
    FROM {{ ref('skad_installs') }} AS sk
    LEFT OUTER JOIN {{ ref('skad_partner_mapping') }} t ON sk.partner_name = t.partner_name
    WHERE sk.installed_at < {{ var('ds') }}::DATE + INTERVAL '1 day'
    {% if is_incremental() %}
        AND sk.installed_at >= {{ var('ds') }}::DATE - interval '35 days'
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
cte_installs_spend AS (
    SELECT
        cohort_utc AS cohort_day,
        DATE_TRUNC('week', cohort_utc) AS cohort_week,
        DATE_TRUNC('month', cohort_utc) AS cohort_month,
        DATE_TRUNC('year', cohort_utc) AS cohort_year,
        geo,
        client_type,
        network_name,
        campaign_name,
        adgroup_name,
        COALESCE(c.ua_cost, 0) AS ua_cost,
        COALESCE(u.installs, 0) AS installs,
        COALESCE(u.new_user_installs, 0) AS new_user_installs,
        COALESCE(u.old_user_installs, 0) AS old_user_installs,
        COALESCE(u.first_comm, 0) AS first_comm,
        COALESCE(u.d3_retained, 0) AS d3_retained,
        u.w2_retained AS w2_retained,
        u.w2r_prob AS synthetic_w2r,
        u.dau_generation,
        u.ltv365 / NULLIF(u.installs, 0) AS ltv365,
        COALESCE(u.outbound_calls, 0) AS outbound_call_d1,
        COALESCE(u.lp_sessions, 0) AS lp_sessions_gte4_d1,
        COALESCE(u.combined_event, 0) AS combined_event,
        installation_type
    FROM user_generation AS u
    FULL OUTER JOIN ua_spend AS c USING (cohort_utc, geo, client_type, network_name, campaign_name, adgroup_name)
)
----Add the network_name root column to the final dataset to facilitate mapping of all networks belonging to the same partner.
SELECT
    cohort_day,
    cohort_week,
    cohort_month,
    cohort_year,
    geo,
    client_type,
    cpi.network_name,
    campaign_name,
    adgroup_name,
    ua_cost,
    installs,
    new_user_installs,
    old_user_installs,
    first_comm,
    d3_retained,
    w2_retained,
    synthetic_w2r,
    dau_generation,
    ltv365,
    outbound_call_d1,
    lp_sessions_gte4_d1,
    combined_event,
    installation_type,
    map.network_name_root,
    CASE
        WHEN REGEXP_LIKE(campaign_name, '(.+)\\s\\([0-9]{4}[0-9]+\\)')
        THEN TRIM(REGEXP_SUBSTR(campaign_name, '(.+)\\s\\([0-9]+\\)', 1, 1, 'e', 1))
        WHEN cmap.actual_campaign_name IS NOT NULL THEN cmap.display_campaign_name
        ELSE cpi.campaign_name
    END AS updated_campaign_name
FROM cte_installs_spend cpi
LEFT OUTER JOIN {{ ref('network_campaigns_mapping') }} map ON cpi.network_name = map.network_name
LEFT OUTER JOIN {{ ref('campaign_mapping') }} cmap ON cpi.campaign_name = cmap.actual_campaign_name