{{
    config(
        tags=['manual'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    tz.tzid,
    c.value::INT AS h3_res2_cell,
    COUNT(tz.tzid) OVER (PARTITION BY h3_res2_cell) AS tzid_count_in_cell,
    ST_INTERSECTION(H3_CELL_TO_BOUNDARY(h3_res2_cell), tz.geometry) AS geometry
FROM
    (SELECT
        tzid,
        geometry,
        H3_COVERAGE(geometry, 2) AS h3_cells
        FROM {{ source('support', 'timezone_boundary') }}
    ) tz,
    LATERAL FLATTEN(input => h3_cells) c