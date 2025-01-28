{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_utc'
        
    )
}}

/*Some of the spending that were not passed over by adjust are manually entered by the Growth Team, either as credit for some frauds or as missing data by adjust.
Growth Team can enter data at the network, campaign, or adgroup level.
1. If the data is received at the campaign level, the spend amount is divided among the adgroups within that campaign based on the number of installs.
2.If the data is received at the network level, the spend amount will be distributed among the campaign with the most installs.
 */
WITH fixed_spend_groups AS (
SELECT
  cohort_utc,
  client_type,
  network_name,
  campaign_name,
  country_code,
  cost,
  CASE WHEN flag_campaign =1 THEN ROW_NUMBER() OVER(PARTITION BY cohort_utc,client_type,network_name,country_code ORDER BY installs DESC) --When the campaign is not entered in the manual sheet
  ELSE DENSE_RANK() OVER(PARTITION BY cohort_utc,client_type,network_name,campaign_name,country_code ORDER BY installs DESC) end AS rn --when the campaign is entered in the sheet
FROM
(
SELECT DISTINCT
    fs.date_utc                                                                                       AS cohort_utc,
    fs.client_type                                                                                    AS client_type,
    CASE WHEN fs.network_name ='TikTok' THEN 'Tiktok' ELSE fs.network_name END                        AS network_name,
    CASE WHEN fs.campaign_name IS NULL THEN 1 ELSE 0 END                                              AS  flag_campaign,
    COALESCE(fs.campaign_name, kpi.event_campaign)                                                    AS campaign_name,
    SUM(installs) OVER( PARTITION BY fs.date_utc,fs.client_type,fs.network_name,kpi.event_campaign)   AS installs,
    CASE WHEN (fs.campaign_name LIKE '%CAN%' OR fs.campaign_name LIKE '%CA%') THEN 'CA' ELSE 'US' END AS country_code,
    spend                                           AS cost
         
FROM {{ source('ua', 'fixed_daily_spend_appendix') }} fs
LEFT OUTER JOIN {{ ref('adjust_kpis') }} kpi 
ON fs.date_utc=kpi.date_utc
AND fs.client_type=kpi.client_type
AND CASE WHEN fs.network_name ='TikTok' THEN 'Tiktok' ELSE fs.network_name END =kpi.event_network
AND  CASE WHEN (fs.campaign_name LIKE '%CAN%' OR fs.campaign_name LIKE '%CA%') THEN 'CA' ELSE 'US' END =kpi.country_code
AND NVL(fs.campaign_name = kpi.event_campaign,true)
{%- if is_incremental() %}
 WHERE fs.date_utc >= CURRENT_DATE() - INTERVAL '35 days'
{% endif %}
  
)a
)

,fixed_adjustment as
(
SELECT  
  fs.cohort_utc,
  fs.client_type,
  fs.network_name,
  COALESCE(fs.campaign_name,kpi.event_campaign) AS campaign_name ,
  CASE WHEN fs.campaign_name IS NULL THEN kpi.country_code ELSE fs.country_code END  AS country_code,
  kpi.event_adgroup AS adgroup_name,
  kpi.installs,
  CASE WHEN SUM(NVL(kpi.installs, 0))  OVER (PARTITION BY fs.client_type,fs.cohort_utc,fs.network_name,fs.country_code,fs.campaign_name) = 0  
  THEN MAX(fs.cost)OVER (PARTITION BY fs.client_type,fs.cohort_utc,fs.network_name,fs.country_code,fs.campaign_name)/ COUNT(fs.cost) OVER (PARTITION BY fs.client_type,fs.cohort_utc,fs.network_name,fs.country_code,fs.campaign_name) ELSE
  CAST(fs.cost*(kpi.installs/(SUM(kpi.installs) OVER (PARTITION BY fs.client_type,fs.cohort_utc,fs.network_name,fs.country_code,fs.campaign_name))::double) AS NUMERIC(35,20)) END AS cost 
               
 FROM fixed_spend_groups fs
 LEFT OUTER JOIN {{ ref('adjust_kpis') }} kpi
 ON fs.cohort_utc=kpi.date_utc
 AND fs.client_type = kpi.client_type 
 AND fs.network_name = kpi.event_network
 AND NVL(fs.country_code=upper(kpi.country_code),true)
 AND NVL(fs.campaign_name = kpi.event_campaign,true)
 WHERE fs.rn=1
 {%- if is_incremental() %}
  AND kpi.date_utc >= CURRENT_DATE() - INTERVAL '35 days'
 {% endif %}

)


SELECT 
  cohort_utc    AS cohort_utc,
  client_type   AS client_type,
  network_name  AS network_name,
  campaign_name AS campaign_name,
  country_code  AS country_code,
  adgroup_name  AS adgroup_name,
  SUM(installs) AS installs,
  SUM(cost)     AS cost
FROM
(
SELECT 
  f.cohort_utc,
  f.client_type,
  f.network_name,
  f.campaign_name,
  f.country_code,
  f.adgroup_name,
  f.installs,
  f.cost  AS cost
FROM fixed_adjustment f
WHERE f.cost>0

UNION ALL

SELECT 
  f.cohort_utc,
  f.client_type,
  f.network_name,
  f.campaign_name,
  f.country_code,
  f.adgroup_name,
  f.installs,
  (f.cost + k.costs) as cost
FROM fixed_adjustment f
INNER JOIN {{ ref('adjust_kpis') }} k
ON f.cohort_utc=k.date_utc
AND f.client_type=k.client_type
AND f.network_name=k.event_network
AND f.campaign_name=k.event_campaign
AND f.country_code=k.country_code
AND f.adgroup_name=k.event_adgroup
WHERE f.cost<0
) a
GROUP BY 1,2,3,4,5,6