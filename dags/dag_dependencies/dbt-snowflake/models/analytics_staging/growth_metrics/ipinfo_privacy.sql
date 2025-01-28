{{
    config(
        tags=['daily'],
        materialized='table',
    )
}}

-- create an internal copy of table
select start_ip,
end_ip,
join_key,
hosting,
proxy,
tor,
vpn,
current_date as load_date
from 
ext.ipinfo