-- features dsn préalable

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
AND A.dd_datedebutcontrat < '2021-12-20'
AND A.kd_dateextraction < '2021-12-20'
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
INNER JOIN fb00_uda03430.demandeurs_tmp3 B
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
A.residence_dep,
B.dc_nafrev2_etablissement_id AS etbaffectation_naf,
C.dc_nafrev2_etablissement_id AS etblieutravail_naf,
C.dc_naturejuremployeur_id AS etblieutravail_natureempl,
C.dc_codepostaletablissement AS etab_dep
FROM fb00_uda03430.dsn_prealable_tmp4 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) B
ON A.dc_siretetbaffectation = B.kc_siretetablissement
LEFT JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) C
ON A.dc_siretetblieutravail = C.kc_siretetablissement
WHERE B.rk = 1 AND C.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp6
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
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf,
A.etab_dep
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national, dc_siretetbaffectation ORDER BY kd_dateextraction DESC) AS rk
    FROM  fb00_uda03430.dsn_prealable_tmp5) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.features_last_pcs_naf;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_last_pcs_naf
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.pcs AS last_pcs,
A.etblieutravail_naf AS last_naf_lieutrav,
A.etbaffectation_naf AS last_naf_affect,
A.etab_dep,
A.residence_dep
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national ORDER BY dd_datedebutcontrat DESC, kd_dateextraction DESC) AS rk
    FROM  fb00_uda03430.dsn_prealable_tmp6) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.features_had_same_pcs;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_had_same_pcs
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 as had_same_pcs
FROM fb00_uda03430.dsn_prealable_tmp6 A
INNER JOIN (SELECT * FROM fb00_uda03430.rome_fap_pcs WHERE rome = 'D1106') B
ON A.pcs = B.pcs;

DROP TABLE IF EXISTS fb00_uda03430.features_had_pcs_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_had_pcs_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.pcs1,
A.pcs2
FROM fb00_uda03430.pcs_proche A
INNER JOIN (SELECT * FROM fb00_uda03430.rome_fap_pcs WHERE rome = 'D1106') B
ON A.pcs1 = B.pcs;

DROP TABLE IF EXISTS fb00_uda03430.features_had_pcs_proche;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_had_pcs_proche
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 as had_pcs_proche
FROM fb00_uda03430.dsn_prealable_tmp6 A
INNER JOIN fb00_uda03430.features_had_pcs_proche_tmp1 B
ON A.pcs = B.pcs2;

DROP TABLE IF EXISTS fb00_uda03430.features_pcs_counter;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_pcs_counter
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT pcs) AS pcs_counter
FROM fb00_uda03430.dsn_prealable_tmp6
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.features_naf_counter;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_naf_counter
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT etblieutravail_naf) AS naf_counter
FROM fb00_uda03430.dsn_prealable_tmp6
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.dsn_prealable_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_prealable_tmp7
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 AS had_pcs,
B.had_same_pcs,
C.had_pcs_proche,
D.pcs_counter,
E.naf_counter,
F.last_pcs,
F.last_naf_lieutrav,
F.last_naf_affect,
F.etab_dep,
F.residence_dep
FROM fb00_uda03430.dsn_prealable_tmp6 A
LEFT JOIN fb00_uda03430.features_had_same_pcs B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN fb00_uda03430.features_had_pcs_proche C
ON A.kn_individu_national = C.kn_individu_national
LEFT JOIN fb00_uda03430.features_pcs_counter D
ON A.kn_individu_national = D.kn_individu_national
LEFT JOIN fb00_uda03430.features_naf_counter E
ON A.kn_individu_national = E.kn_individu_national
LEFT JOIN fb00_uda03430.features_last_pcs_naf F
ON A.kn_individu_national = F.kn_individu_national;


-- DSN ultérieure

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp1
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
AND A.dd_datedebutcontrat >= '2021-12-20'
AND A.dd_datedebutcontrat < '2021-12-20'
AND A.kd_dateextraction >= '2021-12-20'
AND A.kd_dateextraction < '2021-12-20'
AND A.dc_naturecontrat_id IN ('01', '02', '03', '08');

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp2
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
FROM fb00_uda03430.dsn_ulterieure_tmp1 A
INNER JOIN braff00.pr00_pdsnpe_identitedeclareesal B
ON (A.kd_dateextraction = B.kd_dateextraction
AND A.dn_idtidentitedeclaree = B.kn_idtidentitedeclaree
AND A.dc_siretetbaffectation = B.dc_siretetbaffectation);

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp3
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
FROM fb00_uda03430.dsn_ulterieure_tmp2 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national 
    ORDER BY kd_datemodification DESC) AS rk FROM
    braff00.pr00_ppx005_dcoind_individu) B
ON A.dc_nircertifiecnav = SUBSTR(B.dc_nir, 0, 13)
WHERE B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp4
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
FROM fb00_uda03430.dsn_ulterieure_tmp3 A
INNER JOIN fb00_uda03430.demandeurs_tmp3 B
ON A.kn_individu_national = B.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp5;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp5
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
A.residence_dep,
B.dc_nafrev2_etablissement_id AS etbaffectation_naf,
C.dc_nafrev2_etablissement_id AS etblieutravail_naf,
C.dc_naturejuremployeur_id AS etblieutravail_natureempl,
SUBSTR(B.dc_codepostaletablissement, 0, 2) as etab_dep
FROM fb00_uda03430.dsn_ulterieure_tmp4 A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) B
ON A.dc_siretetbaffectation = B.kc_siretetablissement
LEFT JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_siretetablissement ORDER BY dd_dateextraction DESC) AS rk
    FROM  braff00.pr00_pdsnpe_etablissement) C
ON A.dc_siretetblieutravail = C.kc_siretetablissement
WHERE B.rk = 1 AND C.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp6
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
A.residence_dep,
A.etbaffectation_naf,
A.etblieutravail_naf
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national, dc_siretetbaffectation ORDER BY kd_dateextraction DESC) AS rk
    FROM  fb00_uda03430.dsn_ulterieure_tmp5) A
WHERE A.rk = 1;


DROP TABLE IF EXISTS fb00_uda03430.features_will_pcs;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_will_pcs
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 as will_pcs
FROM fb00_uda03430.dsn_ulterieure_tmp6 A;

DROP TABLE IF EXISTS fb00_uda03430.features_will_pcs_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_will_pcs_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.pcs1,
A.pcs2
FROM fb00_uda03430.pcs_proche A
INNER JOIN (SELECT * FROM fb00_uda03430.rome_fap_pcs WHERE rome = 'D1106') B
ON A.pcs1 = B.pcs;

DROP TABLE IF EXISTS fb00_uda03430.features_will_pcs_proche;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_will_pcs_proche
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 as will_pcs_proche
FROM fb00_uda03430.dsn_ulterieure_tmp6 A
INNER JOIN fb00_uda03430.features_will_pcs_proche_tmp1 B
ON A.pcs = B.pcs2;

DROP TABLE IF EXISTS fb00_uda03430.dsn_ulterieure_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dsn_ulterieure_tmp7
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.will_pcs,
B.will_pcs_proche
FROM fb00_uda03430.features_will_pcs A
LEFT JOIN fb00_uda03430.features_will_pcs_proche B
ON A.kn_individu_national = B.kn_individu_national;
