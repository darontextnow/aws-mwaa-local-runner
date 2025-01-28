{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}


SELECT
    b.client_type,
    b.adjust_id,
    b.installed_at,
    b.installed_at::DATE AS date_utc,
    b.install_number,
    i.app_name,
    t.network_name,
    t.campaign_name,
    t.adgroup_name,
    i.store,
    i.impression_based,
    i.is_organic,
    i.is_untrusted,
    i.match_type,
    i.device_type,
    i.device_name,
    i.os_name,
    i.region_code,
    i.country_code,
    i.country_subdivision,
    i.city,
    i.postal_code,
    i.language,
    i.tracking_limited,
    i.deeplink,
    i.timezone,
    i.connection_type,
    date_part(week, b.installed_at) as install_week_of_year,
    dayofweek(b.installed_at) AS install_dow,
    HOUR(b.installed_at) AS install_hour,
    DATEDIFF('HOUR', i.click_time, i.installed_at) AS click_to_install_hours
FROM {{ ref('ltv_numbered_installs') }} b
JOIN {{ ref('installs') }} i USING (adjust_id, client_type, installed_at)
JOIN {{ ref('adjust_trackers') }} t USING (client_type, tracker)
WHERE

{% if is_incremental() %}
    (b.installed_at >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
{% else %}
    (b.installed_at >= '2018-03-01')
{% endif %}
