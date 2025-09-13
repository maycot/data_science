/*** Selection des DE pris en charge à T = t-N (01/06/2020 car covid) depuis max 2 ans
sur departmt Gironde 33 ***/

DROP TABLE IF EXISTS fb00_uda03430.train_demandeurs_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_demandeurs_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
B.dep,
A.dc_motifinscription_id,
A.dd_dateinscriptionpec,
A.dd_dateannulationpec,
A.kn_numpec,
A.kd_datemodification,
A.dc_structure,
A.dc_typepec_id,
A.dc_soustypepec
FROM braff00.pr00_ppx005_ddiadh_priseencharge A
INNER JOIN fb00_uda03430.ale B
ON A.dc_structure = B.dc_structure
WHERE kd_datemodification <= '2020-12-20';

DROP TABLE IF EXISTS fb00_uda03430.train_demandeurs_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_demandeurs_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.dep,
A.dc_motifinscription_id,
A.dd_dateinscriptionpec,
A.dd_dateannulationpec,
A.kn_numpec,
A.dc_structure,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_soustypepec
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national, kn_numpec
    ORDER BY kd_datemodification DESC) AS rk FROM
    fb00_uda03430.train_demandeurs_tmp1) A
WHERE A.rk = 1;


DROP TABLE IF EXISTS fb00_uda03430.train_demandeurs_tmp21;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_demandeurs_tmp21
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.dep,
A.dc_motifinscription_id,
A.dd_dateinscriptionpec,
A.dd_dateannulationpec,
A.kn_numpec,
A.dc_structure,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_soustypepec
FROM (SELECT *, RANK () OVER (PARTITION BY kn_individu_national ORDER BY
 dd_dateinscriptionpec DESC, kn_numpec DESC) AS rk FROM fb00_uda03430.train_demandeurs_tmp2) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.train_demandeurs_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_demandeurs_tmp3
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.dep,
A.dc_structure,
A.kd_datemodification,
A.kn_numpec
FROM fb00_uda03430.train_demandeurs_tmp21 A
WHERE ((A.dc_typepec_id = 1 AND A.dc_soustypepec = 1)
OR  (A.dc_typepec_id = 1 AND A.dc_soustypepec = 2)
OR (A.dc_typepec_id = 1 AND A.dc_soustypepec = 3))
AND A.dd_dateinscriptionpec > '2018-12-20'
AND A.dd_dateannulationpec IS NULL;

/*** Labellisation pour métier D1106 : a une dsn sur métier entre T et T + 6M ***/

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp1
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
AND A.dd_datedebutcontrat >= '2020-12-20'
AND A.dd_datedebutcontrat < '2021-12-20'
AND A.kd_dateextraction >= '2020-12-20'
AND A.kd_dateextraction < '2021-12-20'
AND A.dc_naturecontrat_id IN ('01', '02', '03', '08')
AND (DATEDIFF(A.dd_datefincontrat, A.dd_datedebutcontrat) > 30
OR DATEDIFF(A.dd_datefincontrat, A.dd_datedebutcontrat) IS NULL);

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp2
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
SUBSTR(B.dc_codepostaldomicile, 0, 2) as residence_dep
FROM fb00_uda03430.train_dsn_tmp1 A
INNER JOIN braff00.pr00_pdsnpe_identitedeclareesal B
ON (A.kd_dateextraction = B.kd_dateextraction
AND A.dn_idtidentitedeclaree = B.kn_idtidentitedeclaree
AND A.dc_siretetbaffectation = B.dc_siretetbaffectation);

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp3
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
A.pcs,
A.dc_unitequotitetravail_id,
A.dn_quotitetravail,
A.dc_siretetbaffectation,
A.dc_siretetblieutravail,
A.dc_nircertifiecnav,
A.residence_dep,
B.dc_nafrev2_etablissement_id AS etbaffectation_naf,
C.dc_nafrev2_etablissement_id AS etblieutravail_naf,
C.dc_naturejuremployeur_id AS etblieutravail_natureempl,
SUBSTR(B.dc_codepostaletablissement, 0, 2) as etab_dep
FROM fb00_uda03430.train_dsn_tmp2 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) B
ON A.dc_siretetbaffectation = B.kc_siretetablissement
LEFT JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) C
ON A.dc_siretetblieutravail = C.kc_siretetablissement
WHERE B.rk = 1 AND C.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp4
AS SELECT DISTINCT
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
A.dc_nircertifiecnav,
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etblieutravail_natureempl,
A.etab_dep
FROM fb00_uda03430.train_dsn_tmp3 A
INNER JOIN (SELECT * FROM fb00_uda03430.rome_fap_pcs WHERE rome = 'D1106') B
ON A.pcs = B.pcs;

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp5;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp5
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
A.dc_nircertifiecnav AS nir,
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etblieutravail_natureempl,
A.etab_dep
FROM fb00_uda03430.train_dsn_tmp4 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national 
    ORDER BY kd_datemodification DESC) AS rk FROM
    braff00.pr00_ppx005_dcoind_individu) B
ON A.dc_nircertifiecnav = SUBSTR(B.dc_nir, 0, 13)
WHERE B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.train_count_bni_per_nir;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_count_bni_per_nir
STORED AS ORC
AS SELECT
nir,
COUNT(distinct kn_individu_national) as bni_counter
FROM fb00_uda03430.train_dsn_tmp5
GROUP BY nir;

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp6
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
A.nir,
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etblieutravail_natureempl,
A.etab_dep
FROM fb00_uda03430.train_dsn_tmp5 A
INNER JOIN fb00_uda03430.train_count_bni_per_nir B
ON A.nir = B.nir
WHERE B.bni_counter < 2;

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp7
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
A.nir,
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etblieutravail_natureempl,
A.etab_dep
FROM fb00_uda03430.train_dsn_tmp6 A
INNER JOIN fb00_uda03430.train_demandeurs_tmp3 B
ON A.kn_individu_national = B.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.train_dsn_tmp8;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_dsn_tmp8
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
A.nir,
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etblieutravail_natureempl,
A.etab_dep
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national 
    ORDER BY kd_dateextraction ASC, dd_datedebutcontrat ASC, dn_quotitetravail DESC) AS rk FROM
    fb00_uda03430.train_dsn_tmp7) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.train_label1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_label1
STORED AS ORC
AS SELECT DISTINCT
kn_individu_national,
etblieutravail_naf,
1 as label
FROM fb00_uda03430.train_dsn_tmp8;

DROP TABLE IF EXISTS fb00_uda03430.train_label0;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_label0
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
0 as label
FROM fb00_uda03430.train_demandeurs_tmp3 A
LEFT JOIN fb00_uda03430.train_label1 B
ON A.kn_individu_national = B.kn_individu_national
WHERE B.kn_individu_national IS NULL;

DROP TABLE IF EXISTS fb00_uda03430.train_label;
CREATE TABLE IF NOT EXISTS fb00_uda03430.train_label
STORED AS ORC
AS SELECT * FROM fb00_uda03430.train_label0
UNION ALL
SELECT kn_individu_national, label FROM fb00_uda03430.train_label1;
