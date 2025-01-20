delete from staging.provider_statistics
where concat(date(date), provider, placement) in (
    select concat(date, provider, placement)
    from (values %s) as temp(date, provider, placement)
)