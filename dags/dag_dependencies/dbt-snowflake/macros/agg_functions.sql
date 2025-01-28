{% macro agg_avg_days_between(col_name) -%}

{% set expr -%}
CASE WHEN ZEROIFNULL({{ col_name }}) > 0 THEN date_utc END
{%- endset %}

DATEDIFF(DAY, MIN({{ expr }}), MAX({{ expr }})) / NULLIFZERO(COUNT({{ expr }}) - 1) AS "AVG_DAYS_BW({{ col_name }})"
{%- endmacro %}

{% macro agg_days_from_last(col_name, anchor_date) -%}

{% set expr -%}
MAX(CASE WHEN ZEROIFNULL({{ col_name }}) > 0 THEN date_utc END)
{%- endset %}

DATEDIFF(DAY, {{ expr }}, {{ anchor_date }}) AS "DAYS_FROM_LAST({{ col_name }})"

{%- endmacro %}

{% macro agg_days_from_last_text(col_name, anchor_date) -%}

{% set expr -%}
MAX(CASE WHEN {{ col_name }} <> '' THEN date_utc END)
{%- endset %}

DATEDIFF(DAY, {{ expr }}, {{ anchor_date }}) AS "DAYS_FROM_LAST({{ col_name }})"

{%- endmacro %}

{% macro agg_last(col_name) -%}

ARRAY_AGG({{ col_name }}) WITHIN GROUP (ORDER BY date_utc DESC)[0]::TEXT AS "LAST({{ col_name }})"

{%- endmacro %}

{% macro agg_mode(col_name) -%}

MODE({{ col_name }}) AS "MODE({{ col_name }})"

{%- endmacro %}

{% macro agg_stddev(col_name) -%}

STDDEV({col_name}) AS "STDDEV({{ col_name }})"

{%- endmacro %}

{% macro agg_unique_count(col_name) -%}
ARRAY_SIZE(ARRAY_UNIQUE_AGG(SPLIT({{ col_name }}, ', '))) AS "NUNIQUE({{ col_name }})"
{%- endmacro %}

{% macro agg_window_count(col_name, window, anchor_date) -%}

{% set cond -%}
date_utc BETWEEN DATEADD(DAY, -{{ window }}, {{ anchor_date }}) AND {{ anchor_date }}
{%- endset %}

SUM(CASE WHEN {{ cond }} AND {{ col_name }} > 0 THEN 1 ELSE 0 END) AS "COUNT({{ col_name }})_{{ window }}d"
{%- endmacro %}

{% macro agg_window_sum(col_name, window, anchor_date) -%}

{% set cond -%}
date_utc BETWEEN DATEADD(DAY, -{{ window }}, {{ anchor_date }}) AND {{ anchor_date }}
{%- endset %}

SUM(CASE WHEN {{ cond }} THEN {{ col_name }} ELSE 0 END) AS "SUM({{ col_name }})_{{ window }}d"
{%- endmacro %}