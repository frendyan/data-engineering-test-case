DO
$$
BEGIN
    IF EXISTS (
        select
            date,
            provider,
            placement
        from warehouse.fact_advertisement_statistic
        group by 1,2,3
        having count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Duplicates found!';
    END IF;
END;
$$;