DO
$$
BEGIN
    IF EXISTS (
        select
            app,
            application_id,
            provider,
            placement
        from warehouse.dim_provider
        group by 1,2,3,4
        having count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Duplicates found!';
    END IF;
END;
$$;