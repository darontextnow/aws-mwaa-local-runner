{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_utc'
    )
}}

WITH kpi AS (

    SELECT DISTINCT adj.date_utc                 AS cohort_utc,
                    adj.client_type              AS client_type,
                    adj.tracker                  AS adjust_tracker,
                    adj.country_code             AS country_code,
                    adj.device_type              AS device_type,
                    adj.installs                 AS installs,
                    adj.event_network            AS event_network,
                    CASE WHEN event_network IN ('{{ ua_third_party_cost_providers() | join('\', \'') }}') OR date_utc < '2019-05-01'::DATE THEN 0
                            ELSE adj.costs
                    END                          AS cost
    FROM {{ ref('adjust_kpis') }} adj
    WHERE event_network NOT IN {{ ua_zero_cost_networks() }}
      AND adj.client_type IN {{ mobile_client_types() }}
      AND (NVL(installs, 0) > 0 OR NVL(cost, 0) > 0)
    {% if is_incremental() -%}
      

     AND adj.date_utc >= (SELECT MAX(i.cohort_utc) - INTERVAL '50 days' FROM {{ this }} i)
    {% endif %}
    ORDER BY cohort_utc

),

cost_updates AS (

    SELECT cohort_utc           AS cohort_utc,
           client_type          AS client_type,
           adjust_tracker       AS adjust_tracker,
           SUM(cost)            AS cost
    FROM kpi
    WHERE (event_network ILIKE '%google%' OR
           event_network ILIKE '%facebook%' OR
           event_network ILIKE '%snapchat%' OR
           event_network IN ('Adwords UAC Installs',
                             'Adwords Search Installs',
                             'Instagram Installs',
                             'Spotad',
                             'Apple Search Ads',
                             'Kaden',
                             'Kayzen2'))
    GROUP BY cohort_utc, client_type, adjust_tracker
    ORDER BY cohort_utc

)

SELECT MD5(cohort_utc || client_type || adjust_tracker || country_code ) AS line_item_id, --Device Type Not being available in new Adjust KPI,so that's been omitted from line_item_id
       cohort_utc,
       client_type,
       adjust_tracker,
       country_code,
       device_type,
       installs,
       CASE WHEN sans.adjust_tracker IS NOT NULL AND SUM(kpi.installs) OVER (PARTITION BY kpi.cohort_utc, kpi.client_type, kpi.adjust_tracker) > 0
                THEN sans.cost * RATIO_TO_REPORT(kpi.installs) OVER (PARTITION BY kpi.cohort_utc, kpi.client_type, kpi.adjust_tracker)
            WHEN sans.adjust_tracker IS NOT NULL
                THEN MAX(sans.cost) OVER (PARTITION BY kpi.cohort_utc, kpi.client_type, kpi.adjust_tracker) /
                    COUNT(kpi.installs) OVER (PARTITION BY kpi.cohort_utc, kpi.client_type, kpi.adjust_tracker)
            ELSE kpi.cost END AS cost
FROM kpi
LEFT JOIN cost_updates sans USING (cohort_utc, client_type, adjust_tracker)
ORDER BY cohort_utc
