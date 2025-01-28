{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_utc'
    )
}}

/*Because the appier spend data we receive is at the campaign level rather than the adgroup level,
 in order to calculate CPI cost at the adgroup level, we distribute the campaign level spend data across the adgroups within the campaign based on the number of installs.
 */

WITH Appier_Cost AS (
SELECT 
  date_utc         AS date_utc,
  client_type      AS client_type,
  'Appier'         AS network_name,
  country_code     AS country_code,
  event_campaign   AS campaign_name,
  SUM(costs)       AS cost
FROM {{ ref('adjust_kpis') }}
WHERE event_network = 'Appier'
AND date_utc >= '2023-03-06'
{%- if is_incremental() %}
AND date_utc>= CURRENT_DATE() - INTERVAL '35 days'
{% endif %}
GROUP BY date_utc,client_type,country_code,event_campaign
HAVING   SUM(costs)  >0

),

installs AS (
SELECT
  i.installed_at::DATE AS date_utc,
  upper(i.country_code) AS country_code,
  i.client_type,
  t.network_name,
  COALESCE(t.campaign_name, '') AS campaign_name,
  COALESCE(t.adgroup_name, '') AS adgroup_name,
  COUNT(i.adjust_id) AS installs
FROM {{ref('installs')}} AS i
JOIN {{ref('adjust_trackers')}} AS t USING (client_type, tracker)
WHERE  t.network_name='Appier' AND i.installed_at::DATE >= '2023-03-06'
{%- if is_incremental() %}
AND i.installed_at::date>= CURRENT_DATE() - INTERVAL '35 days' 
{% endif %}
GROUP BY 1,2,3,4,5,6
)



SELECT  
  c.date_utc                                                                       AS cohort_utc,
  c.client_type                                                                    AS client_type,
  c.network_name                                                                   AS network_name,
  c.country_code                                                                   AS country_code,
  c.campaign_name                                                                  AS campaign_name,
  NVL(i.adgroup_name,'')                                                           AS adgroup_name,
  NVL(i.installs  ,0)                                                              AS installs,
  CASE WHEN SUM(NVL(i.installs, 0))  over (partition by c.client_type,c.date_utc,c.network_name,c.country_code,c.campaign_name) = 0  
                THEN MAX(c.cost)over (partition by c.client_type,c.date_utc,c.network_name,c.country_code,c.campaign_name)/ COUNT(c.cost) over (partition by c.client_type,c.date_utc,c.network_name,c.country_code,c.campaign_name) else
           cast(c.cost*(i.installs/(sum(i.installs) over (partition by c.client_type,c.date_utc,c.network_name,c.country_code,c.campaign_name))::double) as numeric(35,20)) end as cost 
               
FROM Appier_Cost c
LEFT JOIN installs i USING (date_utc, client_type, network_name,country_code,campaign_name)