{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_utc'
    )
}}


--This model calculates the final cost,manual overrides + adjust_cost
--For a certain time period Appreciate data was pulled from Appreciate network API directly until we switched to adjust report API
WITH ua_Appreciate_adjustment AS(

    SELECT 
        date_utc as cohort_utc,
        client_type,
        'Appreciate' as network_name,
        CASE WHEN upper(campaign_name) like '%- CA%' then 'CA' else 'US' end as country_code,
        NVL(campaign_name,'') as campaign_name,
        '' as adgroup_name,
        cost

   FROM  {{ source('ua', 'appreciate') }}
   WHERE date_utc<'2022-03-01' and cost>0

)

, ua_smadex_adjustment AS(

    SELECT 
        date_utc as cohort_utc,
        client_type,
        'Smadex' as network_name,
        CASE WHEN upper(campaign_name) like '%_CA%' then 'CA' else 'US' end as country_code,
        NVL(campaign_name,'') as campaign_name,
        '' as adgroup_name,
        cost

   FROM  {{ source('ua', 'smadex') }}
   WHERE date_utc>='2022-01-01' and  date_utc<='2022-07-31' and cost>0

)


--For a certain time period Google Ads ACI and Apple Search Ads spend data did not have right country code,so we fetched
-- from installs data and used that country code 
, google_apple_installs
as
(

  SELECT date_utc         AS date_utc,
         client_type      AS client_type,
         tracker          AS adjust_tracker,
         event_network    AS event_network,
         country_code     AS country_code,
         device_type      AS device_type,
         event_campaign   AS event_campaign,
         event_adgroup    AS event_adgroup,
         event_creative   AS event_creative,
         SUM(installs)    AS installs
  FROM {{ ref('adjust_kpis') }} 
  WHERE event_network in('Google Ads ACI','Apple Search Ads')
    AND client_type IN ('TN_ANDROID', 'TN_IOS_FREE', 'TN_IOS', '2L_ANDROID')
    AND date_utc >= '2020-10-22' and date_utc<'2022-03-01'
  GROUP BY date_utc, client_type, adjust_tracker, event_network, country_code, device_type,
           event_campaign, event_adgroup, event_creative

)
,google_apple_costs as
(

  SELECT date_utc         AS date_utc,
         client_type      AS client_type,
         event_network    AS event_network,
         event_campaign   AS event_campaign,
         event_adgroup    AS event_adgroup,
         country_code     AS country_code,
         SUM(costs)       AS costs
  FROM {{ ref('adjust_kpis') }} 
  WHERE event_network in('Google Ads ACI','Apple Search Ads')
    AND client_type IN ('TN_ANDROID', 'TN_IOS_FREE', 'TN_IOS', '2L_ANDROID')
    AND date_utc >= '2020-10-22' and date_utc<'2022-03-01'
  GROUP BY date_utc, client_type,event_network, event_campaign, event_adgroup,country_code
  HAVING SUM(costs) > 0

)
,ua_google_apple_ads_aci_adjustment as
(
SELECT

    installs.date_utc    as cohort_utc    ,                                                               
    installs.client_type      ,                                                             
    installs.event_network as network_name,
    installs.event_campaign as campaign_name,
    upper(installs.country_code)  as country_code,
    installs.event_adgroup as adgroup_name,
    NVL(costs * RATIO_TO_REPORT(installs.installs) OVER
         (PARTITION BY installs.date_utc, installs.client_type,installs.event_network, installs.event_campaign, installs.event_adgroup), 0)      AS cost 
FROM google_apple_installs installs
LEFT JOIN google_apple_costs
USING (date_utc, client_type, event_network,event_campaign, event_adgroup)
WHERE installs.date_utc<'2022-03-01'

)

 --Adjust sent wrong numbers for this date for Criteo,so overriding it using manual sheet,also adjust sent incorrect data for smadex from period 2022-01-01 until 2022-07-31
 --so eliminating that data
,adjust_kpis as

( --Get the data aggregated at adgroup level, to avoid duplication of data when doing the outer join
  SELECT 
    cohort_utc,
    client_type,
    country_code,
    network_name,
    campaign_name,
    adgroup_name,
    SUM(cost) AS cost
  FROM
  (  SELECT 
        date_utc  AS cohort_utc,
        client_type,
        upper(Country_code) AS country_code,
        event_network AS network_name,
        CASE WHEN event_network='Apple Search Ads'  AND event_campaign='Brand Keywords (18188611)' THEN 'Brand Keywords, US (18188611)' ELSE event_campaign end as campaign_name,
        event_adgroup AS adgroup_name ,
        CASE WHEN event_network='Smadex' AND date_utc>='2022-01-01' AND date_utc<='2022-07-31' THEN  0
        WHEN event_network='Appier' AND date_utc>='2023-03-06' THEN 0 
        WHEN event_network='Criteo' AND date_utc='2022-12-14' THEN 0 ELSE costs END AS cost

      FROM {{ ref('adjust_kpis') }} 
  )a
  WHERE cost>0
{%- if is_incremental() %}
 AND cohort_utc >= CURRENT_DATE() - INTERVAL '35 days'
 {% endif %}
 GROUP BY  
 cohort_utc,
 client_type,
 country_code,
 network_name,
 campaign_name,
 adgroup_name


  )
   


--Manual override data
,
fixed_spend_appendix_adjustment as
(
SELECT 
      cohort_utc ,
      client_type,
      upper(Country_code) as country_code,
      network_name, 
      campaign_name,
      adgroup_name ,
      cost

FROM {{ ref('ua_fixed_spend_appendix_adjustment') }} 
{%- if is_incremental() %}
WHERE cohort_utc >= CURRENT_DATE() - INTERVAL '35 days'

{% endif %}

)

,cost_attribution_adjustment AS(
SELECT

{% for col in ['cohort_utc', 'client_type', 'country_code', 'network_name', 'campaign_name', 'adgroup_name', 'cost'] -%}
    COALESCE(fs.{{ col }}, ap.{{ col }}, sm.{{ col }}, apr.{{ col }}, gg.{{ col }}, kpi.{{ col }} ) AS {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}
FROM fixed_spend_appendix_adjustment fs
FULL OUTER JOIN ua_Appreciate_adjustment ap USING (cohort_utc,client_type,network_name,campaign_name,country_code,adgroup_name)
FULL OUTER JOIN ua_Smadex_adjustment sm USING (cohort_utc,client_type,network_name,campaign_name,country_code,adgroup_name)
FULL OUTER JOIN {{ ref('ua_appier_cost_adjustment') }} apr USING (cohort_utc,client_type,network_name,campaign_name,country_code,adgroup_name)
FULL OUTER JOIN ua_google_apple_ads_aci_adjustment gg USING (cohort_utc,client_type,network_name,campaign_name,country_code,adgroup_name)
FULL OUTER JOIN adjust_kpis kpi USING (cohort_utc,client_type,network_name,campaign_name,country_code,adgroup_name)
{%- if is_incremental() %}
WHERE COALESCE(fs.cohort_utc, ap.cohort_utc,sm.cohort_utc,apr.cohort_utc, gg.cohort_utc, kpi.cohort_utc) >= CURRENT_DATE() - INTERVAL '35 days'

{% endif %}
)

--Adding network fee structure,additional fees incurred by the network

select 
  cost.cohort_utc ,
  cost.client_type,
  cost.country_code,
  cost.network_name, 
  cost.campaign_name,
  cost.adgroup_name ,
  cost + (NVL(fee.factor,0)*cost) as cost
FROM  cost_attribution_adjustment cost
LEFT OUTER JOIN  {{ source('ua', 'network_fee_structure') }} fee
ON cost.network_name=fee.network_name
AND cost.cohort_utc BETWEEN fee.from_dt AND fee.to_dt
