{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='client_type || adjust_tracker'
    )
}}

WITH base AS (

    SELECT client_type      AS client_type,
           tracker          AS adjust_tracker,
           event_network    AS network_name,
           event_campaign   AS campaign_name,
           event_adgroup    AS adgroup_name,
           event_creative   AS creative_name,
           MIN(date_utc)    AS first_appearance
    FROM {{ ref('adjust_kpis') }}
    WHERE network_name NOT IN {{ ua_zero_cost_networks() }}
    {% if is_incremental() -%}
     AND date_utc >= CURRENT_DATE() - INTERVAL '50 days'
    
    {% endif %}
    GROUP BY client_type, tracker, network_name, campaign_name, adgroup_name, creative_name

),

ua_tracker_list AS (

  SELECT DISTINCT client_type,
                  adjust_tracker,
                  LAST_VALUE(network_name) OVER
                      (PARTITION BY client_type, adjust_tracker ORDER BY first_appearance
                       ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS network_name,
                  LAST_VALUE(campaign_name) OVER
                      (PARTITION BY client_type, adjust_tracker ORDER BY first_appearance
                       ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS campaign_name,
                  LAST_VALUE(adgroup_name) OVER
                      (PARTITION BY client_type, adjust_tracker ORDER BY first_appearance
                       ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS adgroup_name,
                  LAST_VALUE(creative_name) OVER
                      (PARTITION BY client_type, adjust_tracker ORDER BY first_appearance
                       ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS creative_name
  FROM base

)

SELECT client_type                                                  AS client_type,
       adjust_tracker                                               AS adjust_tracker,
       network_name                                                 AS network_name,
       CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN campaign_name
            ELSE NVL(campaign_name, 'MISSING_CAMPAIGN') END         AS campaign_name,
       CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN adgroup_name
            ELSE NVL(adgroup_name, 'MISSING_ADGROUP') END           AS adgroup_name,
       CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN creative_name
            ELSE NVL(creative_name, 'MISSING_CREATIVE') END         AS creative_name
FROM ua_tracker_list