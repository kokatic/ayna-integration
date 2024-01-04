-- create structure
create table public.geonames as (select * from geonamespop g where 1=2)

-- drop all partitions
DO $$ 
DECLARE
    partition_name text;
    sql_statement text;
BEGIN
    FOR partition_name IN 
        SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'geonamespop_parent%'
    LOOP
        sql_statement := format('DROP TABLE IF EXISTS %I;', quote_ident(partition_name));

        -- Print the dynamically generated SQL statement to the log
        RAISE NOTICE 'Dynamic SQL: %', sql_statement;

        -- Execute the dynamic SQL statement to drop the partitioned table
        EXECUTE sql_statement;
    END LOOP;

    -- Commit the transaction to persist changes
    COMMIT;
END $$;

-- create parent table 
CREATE TABLE public.geonamespop_parent (
    timezone text,
    population bigint,
    name text,
    modification_date timestamp,
    longitude numeric,
    latitude numeric,
    geonameid bigint,
    feature_code text,
    feature_class text,
    elevation integer,
    dem integer,
    country_code text,
    cc2 text,
    asciiname text,
    alternatenames text,
    admin4_code text,
    admin3_code text,
    admin2_code text,
    admin1_code text
) PARTITION BY LIST (country_code);

-- Create partitions
DO $$ 
DECLARE
    country_code_value text;
    sql_statement text;
BEGIN
    -- Iterate through unique country codes in the 'geonamespop' table
    FOR country_code_value IN 
        SELECT DISTINCT country_code FROM public.geonamespop
    LOOP
        -- Remove double quotes and trim spaces from country code
        country_code_value := trim(both '"' from country_code_value);

        -- Print debug information to the log
        RAISE NOTICE 'Processing country code: %', country_code_value;

        -- Check if the country code is not empty
        IF length(country_code_value) > 0 THEN
            -- Generate the SQL statement to create a partition
            sql_statement := CONCAT(
                'CREATE TABLE IF NOT EXISTS public.geonamespop_parent_', country_code_value,
                ' PARTITION OF public.geonamespop_parent FOR VALUES IN (', quote_literal(country_code_value), ')'
            );

            -- Print the dynamically generated SQL statement to the log
            RAISE NOTICE 'Dynamic SQL: %', sql_statement;
           EXECUTE sql_statement;
        END IF;
    END LOOP;
END $$;

-- Insertion
DO $$ 
DECLARE
    country_code_value text;
    sql_statement text;
BEGIN
    FOR country_code_value IN 
        SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'geonamespop_parent_%'
    LOOP
        BEGIN
            sql_statement := format(
                'INSERT INTO %I (latitude, geonameid, population, modification_date, elevation, dem, longitude, alternatenames, admin4_code, admin3_code, admin2_code, timezone, admin1_code, name, feature_code, feature_class, country_code, cc2, asciiname) ' ||
                'SELECT latitude::numeric, geonameid::bigint, population::bigint, modification_date::timestamp, ' ||
                'NULLIF(NULLIF(elevation, ''''), ''NULL'')::integer, ' ||
                'NULLIF(dem, ''NULL'')::integer, ' ||
                'longitude::numeric, ' ||
                'NULLIF(alternatenames, ''NULL'')::text, ' ||
                'NULLIF(admin4_code, ''NULL'')::text, ' ||
                'NULLIF(admin3_code, ''NULL'')::text, ' ||
                'NULLIF(admin2_code, ''NULL'')::text, ' ||
                'NULLIF(timezone, ''NULL'')::text, ' ||
                'NULLIF(admin1_code, ''NULL'')::text, ' ||
                'NULLIF(name, ''NULL'')::text, ' ||
                'NULLIF(feature_code, ''NULL'')::text, ' ||
                'NULLIF(feature_class, ''NULL'')::text, ' ||
                'country_code::text, ' ||
                'NULLIF(cc2, ''NULL'')::text, ' ||
                'NULLIF(asciiname, ''NULL'')::text ' ||
                'FROM public.geonamespop WHERE country_code = %L AND population IS NOT NULL', 
                quote_ident(country_code_value), UPPER(substring(country_code_value from 'geonamespop_parent_(.*)'))
            );

            -- Print the dynamically generated SQL statement to the log
            RAISE NOTICE 'Dynamic SQL: %', sql_statement;

            -- Debugging: Check if the variables have the expected values
            RAISE NOTICE 'country_code_value: %', country_code_value;

            -- Execute the dynamic SQL statement to insert data into the partitioned table
            EXECUTE sql_statement;
         -- Commit the transaction to persist changes
    		COMMIT;
        END;
    END LOOP; 
END $$;

-- 
DO $$ 
DECLARE
    partition_name text;
    index_statement text;
BEGIN
    FOR partition_name IN 
        SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'geonamespop_parent_%'
    LOOP
        index_statement := format(
            'CREATE INDEX idx_%I_name ON %I (name)', 
            partition_name, 
            partition_name
        );

        -- Print the dynamically generated index statement to the log
        RAISE NOTICE 'Dynamic Index Statement: %', index_statement;

        -- Execute the dynamic index statement
        EXECUTE index_statement;
    END LOOP;
END $$;

-- Testing

SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'geonamespop_%'

select * from geonamespop_parent_ad where country_code='AD'

INSERT INTO geonamespop_parent_ad (latitude, geonameid, population, modification_date, elevation, dem, longitude, alternatenames, admin4_code, admin3_code, admin2_code, timezone, admin1_code, name, feature_code, feature_class, country_code, cc2, asciiname) SELECT latitude::numeric, geonameid::bigint, population::bigint, modification_date::timestamp, NULLIF(NULLIF(elevation, ''), 'NULL')::integer, NULLIF(dem, 'NULL')::integer, longitude::numeric, NULLIF(alternatenames, 'NULL')::text, NULLIF(admin4_code, 'NULL')::text, NULLIF(admin3_code, 'NULL')::text, NULLIF(admin2_code, 'NULL')::text, NULLIF(timezone, 'NULL')::text, NULLIF(admin1_code, 'NULL')::text, NULLIF(name, 'NULL')::text, NULLIF(feature_code, 'NULL')::text, NULLIF(feature_class, 'NULL')::text, country_code::text, NULLIF(cc2, 'NULL')::text, NULLIF(asciiname, 'NULL')::text FROM public.geonamespop WHERE country_code = 'AD' AND population IS NOT NULL
-- Active queries
DO $$ 
DECLARE
    active_pid integer;
BEGIN
    -- Terminate all connections to the database
    WHILE EXISTS (SELECT 1 FROM pg_stat_activity WHERE state = 'active')
    LOOP
        -- Find and terminate one active connection at a time
        SELECT pid INTO active_pid
        FROM pg_stat_activity
        WHERE state = 'active'
        ORDER BY pg_stat_activity.backend_start ASC
        LIMIT 1;

        RAISE NOTICE 'Terminating connection with PID: %', active_pid;

        PERFORM pg_terminate_backend(active_pid);

        -- Wait for a moment before checking again
        PERFORM pg_sleep(1);
    END LOOP;

    -- Proceed with the partitioning script
    -- ... (Your existing partitioning logic here) ...
END $$;

SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    client_hostname,
    client_port,
    backend_start,
    query_start,
    state,
    query
FROM 
    pg_stat_activity
WHERE 
    state = 'active' AND 
    query NOT ILIKE '%pg_stat_activity%'
ORDER BY 
    query_start;












