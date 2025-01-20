-- write to temp table
create table warehouse.dim_provider_tmp as
select
    app,
    application_id,
    provider,
    placement
from staging.provider_lists
;

-- drop existing table
drop table if exists warehouse.dim_provider
;

-- rename tmp table
alter table warehouse.dim_provider_tmp rename to dim_provider
;