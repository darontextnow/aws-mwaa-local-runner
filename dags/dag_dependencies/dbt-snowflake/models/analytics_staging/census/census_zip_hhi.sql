{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
    geo_id,
    zcta,
    hhi,
    hhi_10k_less,
    hhi_10k_15k,
    hhi_15k_25k,
    hhi_25k_35k,
    hhi_35k_50k,
    hhi_50k_75k,
    hhi_75k_100k,
    hhi_100k_150k,
    hhi_150k_200k,
    hhi_200k_more,
    hhi_median,
    hhi_mean
FROM {{ source('census', 'census_hhi_acsst5y2022_s1901') }}
WHERE geo_id NOT ILIKE '%GEO%'
