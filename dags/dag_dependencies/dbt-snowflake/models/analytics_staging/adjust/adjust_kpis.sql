{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}
{%- set metrics = ['impressions', 'clicks', 'installs', 'reattributions', 'sessions', 'cost'] -%}

--Google,Apple ,Tiktok and snapchat in few cases send their adpsend data in adpsend networks and installs in other networks,so all the adpsend networks are renamed
--to networks with installs,so that it would be easier to calculate cpi values(spend/installs) 

    with final_base AS (
        SELECT b.date_utc                                 AS dt,
            case when b.app_name_dashboard ='TextNow - Android' then 'TN_ANDROID'  when b.app_name_dashboard ='TextNow - iOS' then 'TN_IOS_FREE'  when b.app_name_dashboard ='2ndLine - Android' then '2L_ANDROID'end AS client_type,
            NVL(b.tracker,'Unknown')                      AS tracker,
            ''                                            AS device_type,
            NVL(upper(b.country_code), '')                AS country_code,
            case when network_name = 'Google Ads (Ad Spend)' then 'Google Ads ACI' 
                    when network_name='Apple (Ad Spend)' then 'Apple Search Ads' 
                    when network_name='TikTok for Business (Ad Spend)' then 'Tiktok Business'
                    when network_name='Facebook (Ad Spend)' then 'Facebook Installs'
                    when network_name='Snapchat (Ad Spend)' then 'Snapchat Installs' 
                    when network_name='Smadex (Ad Spend)' then 'Smadex' else network_name 
                    end                                   AS event_network,
            b.campaign_name                               AS event_campaign,
            b.adgroup                                     AS event_adgroup,
            ''                                            AS event_creative,
            {% for metric in metrics -%}
            SUM({{ metric }})                             AS {{ metric }}
            {%- if not loop.last %},{% endif %}           
            {% endfor %}
        FROM   {{ source('adjust', 'reportservice_kpi') }}  as b
        WHERE TRIM(NVL(b.network_name, '')) <> '' and TRIM(NVL(b.country_code,''))!='zz'

        {% if is_incremental() %}
       
       and date_utc >= (SELECT MAX(date_utc) - interval '21 days' FROM {{ this }})
        {% endif %}

        GROUP BY date_utc, client_type, tracker, country_code,
                network_name, campaign_name, adgroup

    )

    SELECT dt                                               AS date_utc,
        client_type                                      AS client_type,
        tracker                                          AS tracker,
        device_type                                      AS device_type,
        country_code                                     AS country_code,
        NVL(base.event_network, '')    AS event_network,
        NVL(base.event_campaign, '')  AS event_campaign,
        NVL(base.event_adgroup, '')    AS event_adgroup,
        NVL(base.event_creative,'')  AS event_creative,
        {% for metric in metrics -%}
        {% if metric == 'cost' %}
        COALESCE(base.{{ metric }}, 0) AS {{ metric }}s
        {% else %}
        COALESCE(base.{{ metric }}, 0) AS {{ metric }}
        {% endif %}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
      
    FROM final_base base
    ORDER BY dt


