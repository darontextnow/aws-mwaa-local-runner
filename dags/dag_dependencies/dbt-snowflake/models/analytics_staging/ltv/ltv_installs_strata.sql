{# This model is intended for manual runs for LTV model refresh hence enabled is set to false #}
{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='install_date',
        snowflake_warehouse='PROD_WH_LARGE',
        enabled=false
    )
}}

{% if is_incremental() %}
  {% set query %}
      SELECT MAX(date_utc) - INTERVAL '30 days' FROM {{ this }}
  {% endset %}

  {% if execute %}
      {% set max_dt = run_query(query).columns[0][0] %}
  {% else %}
      {% set max_dt = '2020-01-01' %}
  {% endif %}
{% endif %}


-- installs are put into strata based on their cohort, client_type, paid/organic, DAU in week 0, geo and install_number

SELECT
    a.client_type,
    a.adjust_id,
    a.user_set_id,
    a.install_number,
    a.installed_at::DATE AS install_date,
    LEAST(ROUND(SUM(dau), 0), 7) AS w1_dau,
    HASH(a.installed_at::DATE, a.client_type, a.is_organic, w1_dau, a.geo, a.install_number) AS strata
FROM {{ ref('ltv_numbered_installs') }} a
JOIN {{ ref('dau_user_set_active_days') }} AS d ON
    (d.client_type = a.client_type)
    AND (d.user_set_id = a.user_set_id)
    AND (d.install_date >= a.installed_at::DATE)
WHERE
    (a.installed_at < {{ var('current_date') }} - INTERVAL '7 DAYS')

{% if is_incremental() %}
    AND (a.installed_at >= '{{ max_dt }}'::DATE)
    AND (d.install_date >= '{{ max_dt }}'::DATE)
{% endif %}

    AND (DATEDIFF(DAY, a.installed_at, d.install_date) BETWEEN 0 AND 6)
    AND (d.user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))

GROUP BY 1, 2, 3, 4, 5, 7
