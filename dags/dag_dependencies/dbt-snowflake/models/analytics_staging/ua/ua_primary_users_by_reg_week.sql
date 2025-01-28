{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='active_date',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

{% if is_incremental() %}
  {% set query %}
      SELECT MAX(active_date) - INTERVAL '10 days' FROM {{ this }}
  {% endset %}

  {% if execute %}
      {% set max_dt = run_query(query).columns[0][0] %}
  {% else %}
      {% set max_dt = '2016-05-01' %}
  {% endif %}
{% endif %}
--User's active days along with registration date, active date and the week end date(calculated based on the registration date)
WITH date_range AS (
    SELECT date_utc AS calendar_date
    FROM  {{ source('support', 'dates') }}
    WHERE (date_utc < CURRENT_DATE())

    {% if is_incremental() %}
        AND (date_utc >= '{{ max_dt }}'::DATE)
    {% else %}
        AND (date_utc >= '2016-05-01')
    {% endif %}
),
users_with_active_date AS (
    SELECT
        a.username,
        a.created_at,
        a.active_date,
        b.set_uuid,
        c.first_client_type AS client_type
   FROM {{ ref('username_active_days') }} a
   INNER JOIN {{ source('dau', 'user_set') }} b ON (a.username = b.username)
   INNER JOIN {{ ref('user_sets') }} c ON (b.set_uuid = c.user_set_id)
   WHERE
        (b.set_uuid NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))
        AND (c.first_country_code IN ('US', 'CA'))

   {% if is_incremental() %}
        AND (a.active_date >= '{{ max_dt }}'::DATE)
   {% endif%}
)

SELECT DISTINCT
     u.created_at AS cohort_utc,
     u.username,
     u.set_uuid AS user_set_id,
     u.client_type,
     CASE WHEN active_date = cohort_utc AND DATEADD(DAY, 6, cohort_utc ) < CURRENT_DATE THEN DATEADD(DAY, 6, cohort_utc )
          WHEN DATEDIFF(DAY, cohort_utc, DATEADD(DAY, 1, d.calendar_date)) %7 = 0 AND d.calendar_date > cohort_utc
          AND d.calendar_date < CURRENT_DATE THEN d.calendar_date END AS report_date,
     active_date
FROM date_range d
LEFT OUTER JOIN users_with_active_date u ON
    (DATEDIFF(DAY, active_date, d.calendar_date) BETWEEN 0 AND 6)
WHERE (report_date IS NOT NULL)
ORDER BY username, report_date
