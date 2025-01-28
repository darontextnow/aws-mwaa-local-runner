{{
    config(
        tags=['manual'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    z.zcta5ce20 AS zcta5,
    c.value::INT AS h3_res6_cell,
    COUNT(z.zcta5ce20) OVER (PARTITION BY h3_res6_cell) AS zcta_count_in_cell,
    z.geometry
FROM
    (SELECT
        *,
        H3_COVERAGE(geometry, 6) AS h3_cells
        FROM {{ source('support', 'tl_2023_us_zcta520') }}
    ) z,
    LATERAL FLATTEN(input => h3_cells) c