DROP TABLE IF EXISTS fb00_uda03430.new_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.new_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.dc_motifinscription_id,
A.dd_dateinscriptionpec,
A.dd_dateannulationpec,
A.kn_numpec,
A.dc_structure,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_soustypepec
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national ORDER BY kd_datemodification ASC) AS rk FROM
    braff00.pr00_ppx005_ddiadh_priseencharge) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.new_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.new_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.dc_motifinscription_id,
A.dd_dateinscriptionpec,
A.dd_dateannulationpec,
A.kn_numpec,
A.dc_structure,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_soustypepec
FROM fb00_uda03430.new_tmp1 A
where A.kd_datemodification > '2021-11-01'
AND A.dd_dateinscriptionpec > '2021-11-01'
AND A.dd_dateinscriptionpec < '2021-12-01';


DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_numerocontrat,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.dc_motifrupturecontrat_id,
A.dc_statutsalarie_id,
A.dc_emploi,
A.dc_profcatsoc_id,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_idtdeclarationactivite ORDER BY kd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_declaractivitesal) A
WHERE A.rk = 1
AND A.kd_dateextraction > '2021-11-01';
AND A.dc_naturecontrat_id IN ('01', '02', '03', '08');

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_numerocontrat,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.dc_motifrupturecontrat_id,
A.dc_statutsalarie_id,
A.dc_emploi,
A.dc_profcatsoc_id as pcs,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
B.dc_nircertifiecnav,
B.dc_codepostaldomicile as residence_dep
FROM fb00_uda03430.dsn_prealable_tmp1 A
INNER JOIN braff00.pr00_pdsnpe_identitedeclareesal B
ON (A.kd_dateextraction = B.kd_dateextraction
AND A.dn_idtidentitedeclaree = B.kn_idtidentitedeclaree
AND A.dc_siretetbaffectation = B.dc_siretetbaffectation);

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp3
STORED AS ORC
AS SELECT DISTINCT
B.kn_individu_national,
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.dc_motifrupturecontrat_id,
A.dc_statutsalarie_id,
A.dc_emploi,
A.pcs,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.dc_nircertifiecnav AS nir,
A.residence_dep
FROM fb00_uda03430.dsn_prealable_tmp2 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national 
    ORDER BY kd_datemodification DESC) AS rk FROM
    braff00.pr00_ppx005_dcoind_individu) B
ON A.dc_nircertifiecnav = SUBSTR(B.dc_nir, 0, 13)
WHERE B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp4
STORED AS ORC
AS SELECT DISTINCT
B.kn_individu_national,
A.kd_dateextraction,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.dc_motifrupturecontrat_id,
A.dc_statutsalarie_id,
A.dc_emploi,
A.pcs,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.nir,
A.residence_dep
FROM fb00_uda03430.dsn_prealable_tmp3 A
INNER JOIN fb00_uda03430.new_tmp2 B
ON A.kn_individu_national = B.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp5;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp5
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kd_dateextraction,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.dc_motifrupturecontrat_id,
A.dc_statutsalarie_id,
A.dc_emploi,
A.pcs,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.nir,
A.residence_dep
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national 
    ORDER BY dd_datedebutcontrat ASC, dd_datefincontrat ASC) AS rk FROM
    fb00_uda03430.dsn_prealable_tmp4) A
WHERE A.rk=1;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp6
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kd_dateextraction,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.pcs,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.nir
FROM fb00_uda03430.dsn_prealable_tmp5 A;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp7
STORED AS ORC
AS SELECT DISTINCT
B.kn_individu_national,
A.kd_dateextraction,
A.dc_naturecontrat_id,
A.dd_datedebutcontrat,
A.dd_datefincontrat,
A.pcs,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.nir
FROM fb00_uda03430.new_tmp2 B
LEFT JOIN fb00_uda03430.dsn_prealable_tmp6 A
ON A.kn_individu_national = B.kn_individu_national;

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e "select * from fb00_uda03430.dsn_prealable_tmp6" > /donapp/uda034/p00/dat/dsn_nouveaux_entrants.csv
