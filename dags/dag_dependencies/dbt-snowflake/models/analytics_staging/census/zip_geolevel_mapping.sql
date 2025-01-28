{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT DISTINCT
    z.zcta,
    d.zip,
    d.dma,
    d.dma_name,
    g.city,
    g.state,
    g.stusab,
    g.county,
    g.country,
    g.county_fips,
    g.community,
    g.community_code,
    g.latitude,
    g.longitude,
    g.accuracy
FROM {{ source('census', 'zip_zcta_mapping') }} z
JOIN {{ source('census', 'zip_usa_geo_mapping') }} g ON g.zip = z.zip_code
JOIN {{ source('census', 'zip_dma_mapping_2020') }} d ON g.zip = d.zip