{{
    config(
        tags=['daily'],
        enabled=false
    )
}}
-- This is generated from dbt model `ltv_dau_split`. Take a long time (> 2.5 hours) to run and will need to be executed on a LARGE warehouse
-- NOTE: you need to first run `dbt run -m ltv_numbered_installs --full-refresh` for this script to work
-- For technical details please see https://enflick-my.sharepoint.com/:p:/g/personal/eric_wong_textnow_com/EfrNN0Vh2FJAg4tc5la8zxYBmQt0ze0fZj4QyILttvqT4w?e=4%3aVNP9o1&at=9

-- TODO: Rewrite this to an incremental form

{% for i in range(20) %}
{# apply blue/green splitting logic recursively. This should cover > 95% of installs #}

create or replace temporary table partitioned_dau_for_install_number_{{ loop.index }} as

with partitioned_dau_sum as (

    select
        a.client_type,
        a.adjust_id,
        a.installed_at,
        d.date_utc,
        a.geo,
{#
    We start from new user installs (install_number = 1) and compile their DAU from dau_user_set_active_days.
    Some fraction of their DAU will be attributed to these new installs.
    Any leftover (unattributed_dau) will be available for the next round of split.
#}
{% if loop.index == 1 %}

        d.dau as dau
    from {{ ref('ltv_numbered_installs') }} a
    join {{ ref('dau_user_set_active_days') }} as d on
        d.client_type = a.client_type and
        d.user_set_id = a.user_set_id and
        d.date_utc >= a.installed_at::date and
        d.user_set_id not in (select set_uuid from dau.bad_sets)
    where a.install_number = 1

{% else %}

        d.unattributed_dau as dau
    from {{ ref('ltv_numbered_installs') }} a
    join split_results as d on
        d.client_type = a.client_type and
        d.next_adjust_id = a.adjust_id and
        d.next_installed_at = a.installed_at and
        d.date_utc >= a.installed_at::date
    where a.install_number = {{ loop.index }} and
        d.install_number = {{ loop.index }} - 1

{% endif %}

)
select
    client_type,
    user_set_id,
    adjust_id,
    installed_at,
    install_number,
    next_adjust_id,
    next_installed_at,
    date_utc,
    datediff(day, installed_at, date_utc) as days_from_install,
    dau,
    case when date_utc >= next_installed_at::date then 'green' else 'blue' end as area,
    least(round(sum(case when days_from_install < 7 then dau else 0 end) over (partition by installed_at::date, client_type, adjust_id), 0), 7) as w1_dau,
    hash(installed_at::date, client_type, is_organic, w1_dau, geo) as strata
from partitioned_dau_sum
join {{ ref('ltv_numbered_installs') }} i using (client_type, adjust_id, installed_at);


{% if loop.index == 1 %}
create or replace temporary table split_results as
{% else %}
insert into split_results
{% endif %}
with days as (
    select
        row_number() over (order by 1) - 1 as days_from_install
    from table(generator(ROWCOUNT => 3000))
),
strata as (
    select distinct
        installed_at::date as install_date,
        client_type,
        adjust_id,
        datediff(day, installed_at::date, next_installed_at::date) as next_install_age,
        strata
    from partitioned_dau_for_install_number_{{ loop.index }}
    where days_from_install <= 30  -- this has to be sufficiently large so that no Adjust IDs are omitted
),
strata_size as (
    select
        strata,
        count(1) as strata_size
    from strata
    group by strata
),
strata_blue_size_by_day as (
    select
        strata,
        days_from_install,
        count(1) as strata_blue_size
    from strata
    join days on days_from_install < coalesce(next_install_age, 9999)
    group by strata, days_from_install
)
select
    a.*,
    strata_size.strata_size,
    sum(dau) over (partition by strata, days_from_install) as strata_dau,
    sum(case when area = 'blue' then dau else 0 end) over (partition by strata, days_from_install) as strata_blue_dau,
    strata_dau - strata_blue_dau as strata_green_dau,
    strata_blue_dau / strata_blue_size as r_t,
    least(1, (r_t * strata_size - strata_blue_dau) / nullifzero(strata_green_dau)) as strata_blue_factor, -- fraction of originally green area that should be attributed to blue. When strata_green_dau = 0, there is nothing to split and total for that day will deviate from agg r_t
    case when area = 'green' then dau * strata_blue_factor else dau end as attributed_dau,  -- DAU belonging to blue areas
    dau - attributed_dau as unattributed_dau  -- DAU belonging to green areas
from partitioned_dau_for_install_number_{{ loop.index }} a
join strata_size using (strata)
join strata_blue_size_by_day using (strata, days_from_install);


{% endfor %}


-- persist results into a real table
create or replace transient table public.eric_ltv_colored_areas_ind_split as select * from split_results;


{% for i in range(20) %}
drop table partitioned_dau_for_install_number_{{ loop.index }};
{% endfor %}
