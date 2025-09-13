DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_profcatsoc_id,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_idtdeclarationactivite ORDER BY kd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_declaractivitesal) A
WHERE A.rk = 1
AND A.dd_datedebutcontrat >= '2010-01-01'
AND A.dc_naturecontrat_id IN ('01', '02', '03', '08')
AND DATEDIFF(A.dd_datefincontrat, dd_datedebutcontrat) >= 30;

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_profcatsoc_id,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
B.dc_nircertifiecnav
FROM fb00_uda03430.pcs_proche_tmp1 A
INNER JOIN braff00.pr00_pdsnpe_identitedeclareesal B
ON (A.kd_dateextraction = B.kd_dateextraction
AND A.dn_idtidentitedeclaree = B.kn_idtidentitedeclaree
AND A.dc_siretetbaffectation = B.dc_siretetbaffectation);

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp3
STORED AS ORC
AS SELECT DISTINCT
A.dc_nircertifiecnav,
A.dc_profcatsoc_id AS pcs
FROM fb00_uda03430.pcs_proche_tmp2 A
WHERE A.dc_nircertifiecnav IS NOT NULL
AND LENGTH(A.dc_nircertifiecnav) > 2;

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp4
STORED AS ORC
AS SELECT DISTINCT
A.dc_nircertifiecnav,
A.pcs
FROM fb00_uda03430.pcs_proche_tmp3 A
WHERE A.pcs <> '9999';

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp5;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp5
STORED AS ORC
AS SELECT
A.pcs as pcs1,
B.pcs AS pcs2
FROM fb00_uda03430.pcs_proche_tmp4 A
INNER JOIN fb00_uda03430.pcs_proche_tmp4 B
ON A.dc_nircertifiecnav = B.dc_nircertifiecnav;

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp6
STORED AS ORC
AS SELECT
A.pcs1,
A.pcs2
FROM fb00_uda03430.pcs_proche_tmp5 A
WHERE A.pcs1 <> A.pcs2;

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche_tmp7
STORED AS ORC
AS SELECT
pcs1,
pcs2,
COUNT(*) AS counter
FROM fb00_uda03430.pcs_proche_tmp6
GROUP BY pcs1, pcs2;

DROP TABLE IF EXISTS fb00_uda03430.pcs_proche;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pcs_proche
STORED AS ORC
AS SELECT
A.pcs1,
A.pcs2
FROM (SELECT *, RANK () OVER (PARTITION BY pcs1 ORDER BY
 counter DESC, pcs2 DESC) AS rk FROM fb00_uda03430.pcs_proche_tmp7) A
WHERE A.rk <= 3;


beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e 'select * from fb00_uda03430.pcs_proche_tmp8' > /donapp/uda034/p00/dat/pcs_proche.csv

DROP TABLE IF EXISTS fb00_uda03430.rome_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.rome_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.dc_nircertifiecnav,
A.pcs,
B.rome
FROM fb00_uda03430.pcs_proche_tmp4 A
INNER JOIN fb00_uda03430.rome_fap_pcs B
ON A.pcs = B.pcs;

DROP TABLE IF EXISTS fb00_uda03430.rome_proche_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.rome_proche_tmp2
STORED AS ORC
AS SELECT
A.rome as rome1,
B.rome AS rome2
FROM fb00_uda03430.rome_proche_tmp1 A
INNER JOIN fb00_uda03430.rome_proche_tmp1 B
ON A.dc_nircertifiecnav = B.dc_nircertifiecnav;

DROP TABLE IF EXISTS fb00_uda03430.rome_proche_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.rome_proche_tmp3
STORED AS ORC
AS SELECT
A.rome1,
A.rome2
FROM fb00_uda03430.rome_proche_tmp2 A
WHERE A.rome1 <> A.rome2;

DROP TABLE IF EXISTS fb00_uda03430.rome_proche_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.rome_proche_tmp4
STORED AS ORC
AS SELECT
rome1,
rome2,
COUNT(*) AS counter
FROM fb00_uda03430.rome_proche_tmp3
GROUP BY rome1, rome2;

DROP TABLE IF EXISTS fb00_uda03430.rome_proche;
CREATE TABLE IF NOT EXISTS fb00_uda03430.rome_proche
STORED AS ORC
AS SELECT
A.rome1,
A.rome2
FROM (SELECT *, RANK () OVER (PARTITION BY rome1 ORDER BY
 counter DESC, rome2 DESC) AS rk FROM fb00_uda03430.rome_proche_tmp4) A
WHERE A.rk <= 3;




DROP TABLE IF EXISTS fb00_uda03430.naf_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.naf_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateextraction,
A.kn_idtdeclarationactivite,
A.dn_idtidentitedeclaree,
A.dc_profcatsoc_id,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.dc_nircertifiecnav,
B.dc_nafrev2_etablissement_id AS etbaffectation_naf,
C.dc_nafrev2_etablissement_id AS etblieutravail_naf
FROM fb00_uda03430.pcs_proche_tmp2 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) B
ON A.dc_siretetbaffectation = B.kc_siretetablissement
LEFT JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) C
ON A.dc_siretetblieutravail = C.kc_siretetablissement
WHERE B.rk = 1 AND C.rk = 1;
