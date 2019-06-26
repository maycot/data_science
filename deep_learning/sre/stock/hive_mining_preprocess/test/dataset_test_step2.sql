/************* SRE-V2 DATASET TEST STEP 2
***************/

set hiveconf: SCHEMA = fb00_uda03423;
set SCHEMA;

set hiveconf: test_start = '2017-07-01';
set hiveconf: test_end = '2017-10-01';

set test_start;
set test_end;

-- Find dcopro_profilprofessionnel data 

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dcopro_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dcopro_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.ale,
B.motins,
B.ctp,
B.datnais,
B.sexe,
B.depcom,
B.consmail,
B.conssms,
B.qpv,
B.montant_indem,
B.duree_indem,
B.date_fin_indem,
E.dc_axetravail_id as axe_trav,
E.dc_topdiplomeobtenu as diplome,
E.dc_nivformation_id as nivfor,
E.dc_salaireminiaccepte as salmt,
E.dc_periodicitesalminaccepte as salunit,
E.dc_romev3_1_id as rome,
E.dc_qualificationemploi_1 as qualif,
E.dc_dureeexperience_1 as exper,
E.dc_tempstravail_id as temps,
E.dc_situationfamille_id as sitmat,
E.dc_distanceoudureedeplacement as mobdist,
E.dc_unitemesuredeplacement as mobunit,
E.dc_typecontratpro_id as contrat,
E.dc_nbreenfantacharge as nb_enf,
E.dc_createurrepreneur as entrep,
E.dc_prioritede_1_id as obligemc,
E.kd_datemodification,
E.kc_profilde
FROM ${hiveconf:SCHEMA}.dataset3_test B
JOIN braff00.pr00_ppx005_dcopro_profilprofessionnel E
WHERE (E.kc_individu_local = B.ident
      AND TRUNC(TO_DATE(E.kd_datemodification),'MM') <= TO_DATE(${hiveconf:test_end}))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset4_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset4_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.ale,
B.motins,
B.ctp,
B.datnais,
B.sexe,
B.depcom,
B.consmail,
B.conssms,
B.qpv,
B.montant_indem,
B.duree_indem,
B.date_fin_indem,
B.axe_trav,
B.diplome,
B.nivfor,
B.salmt,
B.salunit,
B.rome,
B.qualif,
B.exper,
B.temps,
B.sitmat,
B.mobdist,
B.mobunit,
B.contrat,
B.nb_enf,
B.entrep,
B.obligemc
FROM (SELECT *, RANK () OVER (
        PARTITION BY ident ORDER BY kd_datemodification DESC,
        kc_profilde DESC) AS rk FROM
    ${hiveconf:SCHEMA}.dcopro_tmp_test) B
WHERE B.rk = 1
;


-- Find activite declaree in prosodie_logs_actu.data_actualisation_ac

-- 1) inner join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.actu_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.actu_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
TO_DATE(CONCAT(20, E.kc_anneeactualisation , '-',
        E.kc_moisactualisation, '-', 01)) as dateactu,
(CASE WHEN E.dn_toptravail = '1' THEN E.dn_nbheuretravail ELSE 0 END) h_trav_m,
E.dn_salairebrut as s_trav_m
FROM ${hiveconf:SCHEMA}.dataset4_test B
INNER JOIN braff00.pr00_psigma_regimegeneralactu E
WHERE (B.ident = E.kc_individu_local
    AND TO_DATE(CONCAT(20, E.kc_anneeactualisation , '-', E.kc_moisactualisation, '-00')) <= TO_DATE(${hiveconf:test_start})
    AND TO_DATE(CONCAT(20, E.kc_anneeactualisation , '-', E.kc_moisactualisation, '-00')) >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 365))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.actu_tmp2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.actu_tmp2_test
STORED AS ORC
AS SELECT
A.ident,
A.three_months_h_trav,
A.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav
FROM (SELECT ident,
    SUM(h_trav_m) AS three_months_h_trav,
    SUM(s_trav_m) AS three_months_s_trav
    FROM ${hiveconf:SCHEMA}.actu_tmp_test
    WHERE dateactu >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 90)
    GROUP BY ident) A
INNER JOIN (SELECT ident,
    SUM(h_trav_m) as six_months_h_trav,
    SUM(s_trav_m) as six_months_s_trav
    FROM ${hiveconf:SCHEMA}.actu_tmp_test
    WHERE dateactu >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 180)
    GROUP BY ident) B
ON A.ident = B.ident
;


-- 2) left join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset5_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset5_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.ale,
B.motins,
B.ctp,
B.datnais,
B.sexe,
B.depcom,
B.consmail,
B.conssms,
B.qpv,
B.montant_indem,
B.duree_indem,
B.date_fin_indem,
B.axe_trav,
B.diplome,
B.nivfor,
B.salmt,
B.salunit,
B.rome,
B.qualif,
B.exper,
B.temps,
B.sitmat,
B.mobdist,
B.mobunit,
B.contrat,
B.nb_enf,
B.entrep,
B.obligemc,
E.three_months_h_trav,
E.three_months_s_trav,
E.six_months_h_trav,
E.six_months_s_trav
FROM ${hiveconf:SCHEMA}.dataset4_test B
LEFT JOIN ${hiveconf:SCHEMA}.actu_tmp2_test E
ON B.ident = E.ident
;

/*** DATA SUPERVISION ***/

use fb00_uda03423;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'dcopro_tmp_test', count(*)
FROM dcopro_tmp_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'actu_tmp2_test', count(*)
FROM actu_tmp2_test;

DROP TABLE ${hiveconf:SCHEMA}.dataset3_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset4_test;
DROP TABLE ${hiveconf:SCHEMA}.actu_tmp2_test;
DROP TABLE ${hiveconf:SCHEMA}.dcopro_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.actu_tmp_test_test;
