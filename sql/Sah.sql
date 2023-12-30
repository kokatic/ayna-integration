CREATE INDEX idx_geoname_geonameid ON geonames (geonameid);

CREATE INDEX idx_geonamecity_geonameid ON geonamescity (geonameid);

CREATE EXTENSION pg_trgm;
CREATE INDEX idx_geonamescity_name_trgm ON geonamescity USING gin (lower(name) gin_trgm_ops);


select * from geonames g where g."name" like '%Paris%' and feature_code in (
'ADM1',
'ADM1H',
'ADM2',
'ADM2H',
'ADM3',
'ADM3H',
'ADM4',
'ADM4H',
'ADM5',
'ADM5H',
'ADMD',
'ADMDH',
'LTER',
'PCL',
'PCLD',
'PCLF',
'PCLH',
'PCLI',
'PCLIX',
'PCLS',
'PRSH',
'TERR',
'ZN',
'ZNB'
) 

select count(*) from geonames g 

select count(*) from geonamescity g 

select * from geonames g where name like '%Alger%'

create table geonamescity  as (
	select * from geonames where population > 0 and feature_code in (
	'ADM1',
	'ADM1H',
	'ADM2',
	'ADM2H',
	'ADM3',
	'ADM3H',
	'ADM4',
	'ADM4H',
	'ADM5',
	'ADM5H',
	'ADMD',
	'ADMDH',
	'LTER',
	'PCL',
	'PCLD',
	'PCLF',
	'PCLH',
	'PCLI',
	'PCLIX',
	'PCLS',
	'PRSH',
	'TERR',
	'ZN',
	'ZNB',
	'PPL',
	'PPLA',
	'PPLA',
	'PPLA',
	'PPLA',
	'PPLA',
	'PPLC',
	'PPLC',
	'PPLF',
	'PPLG',
	'PPLH',
	'PPLL',
	'PPLQ',
	'PPLR',
	'PPLS',
	'PPLW',
	'PPLX'
	)  
)


create table geonamescity as (
	select * from geonames where feature_code in (
	'PPL'
	) 
)
