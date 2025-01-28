{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

-- * File: persona_census.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query creates an enriched dataset combining user location information
--   with ZIP code metadata, census data, and property value data.

SELECT
    a.username,
    a.zip_code,
    b.median_age,
    CAST(b.total_population AS NUMBER) AS total_population,
    CAST(d.total_urban_pop AS NUMBER) AS total_urban_pop,
    CAST(d.total_rural_pop AS NUMBER) AS total_rural_pop,
    CAST(e.value AS DECIMAL(15, 2)) AS latest_homevalue,
    TRY_CAST(c.hhi AS NUMBER) AS hhi,
    TRY_CAST(c.hhi_10k_less AS NUMBER) AS hhi_10k_less,
    TRY_CAST(c.hhi_10k_15k AS NUMBER) AS hhi_10k_15k,
    TRY_CAST(c.hhi_15k_25k AS NUMBER) AS hhi_15k_25k,
    TRY_CAST(c.hhi_25k_35k AS NUMBER) AS hhi_25k_35k,
    TRY_CAST(c.hhi_35k_50k AS NUMBER) AS hhi_35k_50k,
    TRY_CAST(c.hhi_50k_75k AS NUMBER) AS hhi_50k_75k,
    TRY_CAST(c.hhi_75k_100k AS NUMBER) AS hhi_75k_100k,
    TRY_CAST(c.hhi_100k_150k AS NUMBER) AS hhi_100k_150k,
    TRY_CAST(c.hhi_150k_200k AS NUMBER) AS hhi_150k_200k,
    TRY_CAST(c.hhi_200k_more AS NUMBER) AS hhi_200k_more,
    TRY_CAST(c.hhi_median AS NUMBER) AS hhi_median,
    TRY_CAST(c.hhi_mean AS NUMBER) AS hhi_mean
FROM {{ ref('persona_general_location') }} AS a
LEFT JOIN {{ source('zip_code_meta', 'zip_code_metadata') }} AS b ON (a.zip_code = b.zip)
LEFT JOIN {{ source('support', 'hr_census_hhi_acsst5y2022_s1901_120124') }} AS c ON (a.zip_code = c.zcta)
LEFT JOIN {{ source('support', 'hr_census_population_decennialdhc2020_p2_120124') }} AS d ON (a.zip_code = d.zcta)
LEFT JOIN {{ source('support', 'zillow_zip_homevalue') }} AS e ON (a.zip_code = e.regionname)
