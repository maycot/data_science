set hiveconf: SCHEMA = fb00_uda03430;
set SCHEMA;
set hiveconf: SCHEMA_DATALAKE = braff00;
set SCHEMA_DATALAKE;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.mots_recherche_offres;
CREATE TABLE ${hiveconf:SCHEMA}.mots_recherche_offres
STORED AS ORC
AS SELECT DISTINCT
dd_dateappelapirechercheoffre as date_rech,
da_motscles
FROM ${hiveconf:SCHEMA_DATALAKE}.pr00_pda011_sdrapi_rechercheoffre
WHERE dd_dateappelapirechercheoffre >= '2019-05-01'
AND dd_dateappelapirechercheoffre < '2019-11-01';

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.sdr_rechercheoffre;
CREATE TABLE ${hiveconf:SCHEMA}.sdr_rechercheoffre
STORED AS ORC
AS SELECT DISTINCT
dd_dateappelapirechercheoffre as date_rech,
da_resultatsoffres
FROM ${hiveconf:SCHEMA_DATALAKE}.pr00_pda011_sdrapi_rechercheoffre
WHERE yearraff = '2020'
AND monthraff = '6'
AND dd_dateappelapirechercheoffre < '2020-06-15'
AND da_motscles IS NOT NULL
AND dc_nbrtotalresultrechercheoffre IS NOT NULL
AND da_resultatsoffres IS NOT NULL
ORDER BY dd_dateappelapirechercheoffre ASC;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.sdr_recherchecommune;
CREATE TABLE ${hiveconf:SCHEMA}.sdr_recherchecommune
STORED AS ORC
AS SELECT DISTINCT
dd_dateappelapirechercheoffre as date_rech,
SUBSTRING(dc_communerechercheoffre, 3, 5) as commune_rech
FROM ${hiveconf:SCHEMA_DATALAKE}.pr00_pda011_sdrapi_rechercheoffre
WHERE yearraff = '2020'
AND monthraff IN ('8', '9', '10')
AND da_motscles IS NOT NULL
AND dc_nbrtotalresultrechercheoffre IS NOT NULL
AND da_resultatsoffres IS NOT NULL
AND dc_communerechercheoffre IS NOT NULL;

--hive -e 'set hive.cli.print.header=true;select * from fb00_uda03430.sdr_recherchecommune' | sed 's/[\t]/|/g' > /donapp/uda034/p00/dat/sdr_recherchecommune.csv

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.sdr_filtres;
CREATE TABLE ${hiveconf:SCHEMA}.sdr_filtres
STORED AS ORC
AS SELECT DISTINCT
dd_dateappelapirechercheoffre as date_rech,
ds_filtres
FROM ${hiveconf:SCHEMA_DATALAKE}.pr00_pda011_sdrapi_rechercheoffre
WHERE yearraff = '2019'
AND monthraff IN ('11')
AND da_motscles IS NOT NULL
AND dc_nbrtotalresultrechercheoffre IS NOT NULL
AND da_resultatsoffres IS NOT NULL
AND ds_filtres IS NOT NULL;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.sdr_motscles;
CREATE TABLE ${hiveconf:SCHEMA}.sdr_motscles
STORED AS ORC
AS SELECT DISTINCT
dd_dateappelapirechercheoffre as date_rech,
da_motscles
FROM ${hiveconf:SCHEMA_DATALAKE}.pr00_pda011_sdrapi_rechercheoffre
WHERE yearraff = '2019'
AND monthraff IN ('11')
AND da_motscles IS NOT NULL
AND dc_nbrtotalresultrechercheoffre IS NOT NULL
AND da_resultatsoffres IS NOT NULL
AND dc_communerechercheoffre IS NOT NULL;
