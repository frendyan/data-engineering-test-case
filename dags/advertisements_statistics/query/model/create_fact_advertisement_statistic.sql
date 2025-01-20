-- write to temp table
create table warehouse.fact_advertisement_statistic_tmp as
select
    ps.date,
    dp.app as app_name,
    ps.provider,
    ps.placement,
    ps.impressions,
    cast(ps.clicks as int) as clicks,
    cast(ps.conversions as int),
    round(cast(ps.earnings as numeric), 2) as earnings_usd,
    cast(ps.sdk_bootups as int) as daily_viewers,
    cast(ps.daily_unique_viewers as int) as daily_unique_viewers,
    cast(ps.daily_unique_conversions as int) as daily_unique_conversions,
    cast(ps.e_cpm as float) as e_cpm,
    ps.platform,
    cast(ps.coin_sum as int) as coin_sum
from staging.provider_statistics ps
left join warehouse.dim_provider dp on ps.provider = dp.provider and ps.placement = dp.placement
;

-- drop existing table
drop table if exists warehouse.fact_advertisement_statistic
;

-- rename tmp table
alter table warehouse.fact_advertisement_statistic_tmp rename to fact_advertisement_statistic
;