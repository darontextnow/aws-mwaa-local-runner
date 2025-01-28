{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
    geo_id,
    zcta,
    total_population,
    total_urban_pop,
    total_rural_pop,
    CASE 
        WHEN (total_urban_pop*100/total_population) < 25 THEN '0-25'
        WHEN (total_urban_pop*100/total_population) BETWEEN 25 AND 50 THEN '25-50'
        WHEN (total_urban_pop*100/total_population) BETWEEN 50 AND 75 THEN '50-75'
        WHEN (total_urban_pop*100/total_population) BETWEEN 75 AND 100 THEN '75-100'
    END AS urban_pop_pct_bucket,
    CASE 
        WHEN (total_rural_pop*100/total_population) < 25 THEN '0-25'
        WHEN (total_rural_pop*100/total_population) BETWEEN 25 AND 50 THEN '25-50'
        WHEN (total_rural_pop*100/total_population) BETWEEN 50 AND 75 THEN '50-75'
        WHEN (total_rural_pop*100/total_population) BETWEEN 75 AND 100 THEN '75-100'
    END AS rural_pop_pct_bucket
FROM {{ source('census', 'census_population_decennialdhc2020_p2') }}
WHERE total_population > 0
