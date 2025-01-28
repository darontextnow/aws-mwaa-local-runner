{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH install_callback_trackers AS (

    SELECT DISTINCT client_type,
                    tracker,
                    MAX(installed_at) OVER (PARTITION BY client_type, tracker)  AS latest_ts,
                    LAST_VALUE(network_name IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY installed_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS network_name,
                    LAST_VALUE(NVL(fb_campaign_group_name, campaign_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY installed_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS campaign_name,
                    LAST_VALUE(NVL(fb_campaign_name, adgroup_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY installed_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS adgroup_name,
                    LAST_VALUE(NVL(fb_adgroup_name, creative_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY installed_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS creative_name
    FROM {{ ref('installs_with_pi') }}
    JOIN {{ source('adjust', 'apps') }} USING (app_name)
    WHERE NVL(network_name, '') <> '' and NVL(network_name,'')<>'Unattributed' 
    and NVL(fb_install_referrer_publisher_platform,'') <> 'facebook' --excluding facebook 
      AND tracker IS NOT NULL and NVL(tracker,'') <>'unattr'


      union all
      --retrieving Facebook Trackers from adjust_install_stage as they are generated as part of dbt process

        SELECT DISTINCT b.client_type,
                    b.tracker as tracker,
                    MAX(a.installed_at) OVER (PARTITION BY b.client_type,a.FB_INSTALL_REFERRER_CAMPAIGN_NAME,a.FB_INSTALL_REFERRER_ADGROUP_NAME)  AS latest_ts,
                    'Facebook Installs' network_name,
                    a.FB_INSTALL_REFERRER_CAMPAIGN_NAME      AS campaign_name,
                   a.FB_INSTALL_REFERRER_ADGROUP_NAME       AS adgroup_name,
                   a.FB_INSTALL_REFERRER_ADGROUP_NAME       AS creative_name
    FROM {{ ref('installs_with_pi') }} a
    Left join {{ ref('adjust_installs_stage') }} b on a.adid=b.adjust_id and a.installed_at=b.installed_at and a.app_name=b.app_name
 
    WHERE a.fb_install_referrer_publisher_platform='facebook'
      AND  a.tracker = 'unattr'

),

reattribution_callback_trackers AS (

    SELECT DISTINCT client_type,
                    tracker,
                    MAX(created_at) OVER (PARTITION BY client_type, tracker)  AS latest_ts,
                    LAST_VALUE(network_name IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY created_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS network_name,
                    LAST_VALUE(NVL(fb_campaign_group_name, campaign_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY created_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS campaign_name,
                    LAST_VALUE(NVL(fb_campaign_name, adgroup_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY created_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS adgroup_name,
                    LAST_VALUE(NVL(fb_adgroup_name, creative_name) IGNORE NULLS) OVER
                      (PARTITION BY client_type, tracker ORDER BY created_at
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS creative_name
    FROM {{ ref('reattribution_with_pi') }}
    JOIN {{ source('adjust', 'apps') }} USING (app_name)
    WHERE NVL(network_name, '') <> '' and NVL(network_name,'')<>'Unattributed'  and NVL(fb_install_referrer_publisher_platform,'')<> 'facebook' --excluding facebook 
      AND tracker IS NOT NULL and NVL(tracker,'') <>'unattr'

    union all

      --retrieving Facebook Trackers from adjust_install_stage as they are generated as part of dbt process

        SELECT DISTINCT b.client_type,
                    b.tracker as tracker,
                    MAX(a.installed_at) OVER (PARTITION BY b.client_type,a.FB_INSTALL_REFERRER_CAMPAIGN_NAME,a.FB_INSTALL_REFERRER_ADGROUP_NAME)  AS latest_ts,
                    'Facebook Installs' network_name,
                    a.FB_INSTALL_REFERRER_CAMPAIGN_NAME      AS campaign_name,
                   a.FB_INSTALL_REFERRER_ADGROUP_NAME       AS adgroup_name,
                   a.FB_INSTALL_REFERRER_ADGROUP_NAME       AS creative_name
    FROM {{ ref('reattribution_with_pi') }} a
    Left join {{ ref('adjust_reattributions') }} b on a.adid=b.adjust_id and a.installed_at=b.installed_at and a.app_name=b.app_name
 
       WHERE a.fb_install_referrer_publisher_platform='facebook'
      AND  a.tracker = 'unattr'
     

),

kpi_export_trackers AS (

    SELECT DISTINCT client_type,
                    tracker,
                    CURRENT_TIMESTAMP + INTERVAL '1 day'                                AS latest_ts,
                    LAST_VALUE(event_network) OVER
                      (PARTITION BY client_type, tracker ORDER BY date_utc
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS network_name,
                    LAST_VALUE(event_campaign) OVER
                      (PARTITION BY client_type, tracker ORDER BY date_utc
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS campaign_name,
                    LAST_VALUE(event_adgroup) OVER
                      (PARTITION BY client_type, tracker ORDER BY date_utc
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS adgroup_name,
                    LAST_VALUE(event_creative) OVER
                      (PARTITION BY client_type, tracker ORDER BY date_utc
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)     AS creative_name
    FROM {{ ref('adjust_kpis') }}
    WHERE event_network <> ''
      AND tracker IS NOT NULL
      AND (NVL(installs, 0) > 0 OR NVL(reattributions, 0) > 0 or NVL(costs,0)>0)
      --Added NVL(costs,0)>0  to include networks which has only cost values

),

adjust_tracker_list AS (

    SELECT DISTINCT client_type,
                    tracker,
                    LAST_VALUE(network_name) OVER
                      (PARTITION BY client_type, tracker ORDER BY latest_ts
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)                      AS network_name,
                    NVL(LAST_VALUE(campaign_name) OVER
                      (PARTITION BY client_type, tracker ORDER BY latest_ts
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 'MISSING_CAMPAIGN') AS campaign_name,
                    NVL(LAST_VALUE(adgroup_name) OVER
                      (PARTITION BY client_type, tracker ORDER BY latest_ts
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 'MISSING_ADGROUP')  AS adgroup_name,
                    NVL(LAST_VALUE(creative_name) OVER
                      (PARTITION BY client_type, tracker ORDER BY latest_ts
                        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 'MISSING_CREATIVE') AS creative_name
    FROM (

        /*
         We treat kpi as the best source of truth when it comes to tracker information, callbacks
         should never override it. However for period not covered by kpi, we expect tracker with
         the latest timestamp to populate the table. We could write a more complicate query to
         reflect said logic, but I'm taking the lazy path by assigning a timestamp in the future
         to all kpi trackers and always taking the latest tracker regardless of source.
         */
        {% for tbl in ['kpi_export_trackers', 'install_callback_trackers', 'reattribution_callback_trackers'] %}
        SELECT client_type                                                    AS client_type,
               tracker                                                        AS tracker,
               latest_ts                                                      AS latest_ts,
               network_name                                                   AS network_name,
               CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN campaign_name
                    ELSE NVL(campaign_name, 'MISSING_CAMPAIGN') END           AS campaign_name,
               CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN adgroup_name
                    ELSE NVL(adgroup_name, 'MISSING_ADGROUP') END             AS adgroup_name,
               CASE WHEN network_name IN {{ ua_zero_cost_networks() }} THEN creative_name
                    ELSE NVL(creative_name, 'MISSING_CREATIVE') END           AS creative_name
        FROM {{ tbl }}
        {% if not loop.last %}
        UNION ALL{% endif %}
        {% endfor %}
    ) g

),

custom_tracker_list AS (

    SELECT DISTINCT client_type,
                    'c_' || LEFT(MD5(client_type || network_name || NVL(campaign_name, '')), 10) AS tracker,
                    network_name,
                    campaign_name,
                    NULL AS adgroup_name,
                    NULL AS creative_name
    FROM adjust_tracker_list
    WHERE network_name NOT IN {{ ua_zero_cost_networks() }}

),

tatari_tracker_list AS (

    SELECT DISTINCT
        client_type,
        tracker,
        network_name,
        campaign_name,
        adgroup_name,
        creative_name
    FROM {{ ref('ua_tatari_streaming_installs') }}

)

SELECT *
FROM adjust_tracker_list

UNION ALL

SELECT *
FROM custom_tracker_list

UNION ALL

SELECT *
FROM tatari_tracker_list