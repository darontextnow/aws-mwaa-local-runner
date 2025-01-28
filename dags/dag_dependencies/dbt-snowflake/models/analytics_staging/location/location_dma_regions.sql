{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='geoid'
    )
}}


SELECT
    c.GEOID,
    s.NAME AS STATE_NAME,
    g.NAME AS COUNTY_NAME,
    c.TVDMA,
    g.GEOMETRY
FROM {{ ref('location_county_dma') }} c
JOIN {{ source('support', 'tl_2020_us_county') }} g USING (GEOID)
JOIN {{ ref('location_state_geocodes_v2020') }} s USING (STATEFP)
