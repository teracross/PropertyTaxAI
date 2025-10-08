/*
WARNING - only to be used for clearing out test and locally running instances of PSQL database - use at own risk
*/

DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident('public') || '.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;