CREATE TABLE al2 (
    geonameid BIGINT,
    name TEXT,
    asciiname TEXT,
    alternatenames TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    feature_class VARCHAR(10),
    feature_code TEXT,
    country_code VARCHAR(40),
    cc2 TEXT,
    admin1_code TEXT,
    admin2_code TEXT,
    admin3_code TEXT,
    admin4_code TEXT,
    population BIGINT,
    elevation BIGINT,
    dem TEXT,
    timezone TEXT,
    modification_date DATE
);



SELECT geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date
	FROM public.geonames where name like 'Aflou';
	
SELECT * FROM public.al2
LIMIT 100;
