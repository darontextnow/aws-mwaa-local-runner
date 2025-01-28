{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
    geo_id,
	SPLIT_PART(zcta, ' ', 2) AS zcta,
	total_population,
	white_population,
	black_population,
	american_indian_alaska_population,
	asian_population,
	hawaiian_pacific_population,
	some_other_race_alone,
	two_more_race
FROM {{ source('census', 'census_race_decennialdhc2020_p3') }}
WHERE geo_id NOT ILIKE '%GEO%'
