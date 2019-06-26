/************* SRE-V2 DATASET TEST STEP 1
***************/

set hiveconf: SCHEMA = fb00_uda03423;
set SCHEMA;

set hiveconf: test_start = '2017-07-01';
set hiveconf: test_end = '2017-10-01';

set test_start;
set test_end;

-- Select input for test period

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.ddi_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.ddi_tmp_test
STORED AS ORC
AS SELECT DISTINCT
E.kc_individu_local as ident,
E.kn_individu_national as bni,
E.dc_typepec_id as type_pec,
E.dc_structure as ale,
E.dc_motifinscription_id as motins,
E.dc_planactiondecrpouctp as ctp,
E.kd_datemodification,
E.kn_numpec
FROM braff00.pr00_ppx005_ddi_priseencharge E
WHERE (TO_DATE(E.kd_datemodification) < TO_DATE(${hiveconf:test_end})
    AND TO_DATE(E.kd_datemodification) > DATE_SUB(TO_DATE(${hiveconf:test_end}), 750))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset1_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset1_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.type_pec,
B.ale,
B.motins,
B.ctp
FROM (SELECT *, RANK () OVER (
        PARTITION BY ident
        ORDER BY kd_datemodification DESC, kn_numpec DESC) AS rk FROM
    ${hiveconf:SCHEMA}.ddi_tmp_test) B
WHERE B.rk = 1
;

-- Find refh_individu data 

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.refh_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.refh_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.ale,
B.motins,
B.ctp,
E.dd_datenaissance as datnais,
E.dc_sexe_id as sexe,
E.dc_commune_id as depcom,
E.dc_consentement_mail_id as consmail,
E.dc_consentement_sms_id as conssms,
E.dc_codeqpv_id as qpv,
E.td_datefonctionnelle
FROM ${hiveconf:SCHEMA}.dataset1_test B
JOIN  braff00.pr00_ppx005_refh_individu E
WHERE (B.ident = E.dc_individu_local
    AND TO_DATE(E.td_datefonctionnelle) < TO_DATE(${hiveconf:test_end}))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset2_test
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
B.qpv
FROM (SELECT *, RANK () OVER (
        PARTITION BY ident ORDER BY td_datefonctionnelle DESC) AS rk FROM
    ${hiveconf:SCHEMA}.refh_tmp_test) B
WHERE B.rk = 1
;


-- Find indemnisation : capital and duree

-- 1) inner join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.indemnisation_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.indemnisation_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
E.dn_capitalodcourante,
E.dn_dureedroitcourant,
E.dd_datetheoriquefindroit,
E.kd_datemodification,
E.kc_ouverturedroit,
E.kc_reprisedroit
FROM ${hiveconf:SCHEMA}.dataset2_test B
INNER JOIN braff00.pr00_ppx005_drvder_elementdroitncp E
WHERE (B.bni = E.kn_individu_national
    AND TO_DATE(E.kd_datemodification) < TO_DATE(${hiveconf:test_end}))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.indemnisation_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.indemnisation_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
B.dn_capitalodcourante as montant_indem,
B.dn_dureedroitcourant as duree_indem,
B.dd_datetheoriquefindroit as date_fin_indem
FROM (SELECT *, RANK () OVER (
        PARTITION BY ident ORDER BY kd_datemodification DESC,
        kc_ouverturedroit DESC, kc_reprisedroit DESC) AS rk FROM
    ${hiveconf:SCHEMA}.indemnisation_tmp_test) B
WHERE B.rk = 1
;

-- 2) left join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset3_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset3_test
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
E.montant_indem,
E.duree_indem,
E.date_fin_indem
FROM ${hiveconf:SCHEMA}.dataset2_test B
LEFT JOIN ${hiveconf:SCHEMA}.indemnisation_test E
ON (B.ident = E.ident
    AND B.bni = E.bni)
;


/*** DATA SUPERVISION ***/

use fb00_uda03423;

CREATE TABLE IF NOT EXISTS sre_test_supervision(
year STRING,
month_start STRING,
month_end STRING,
table_name STRING,
ident_count INT
) STORED AS ORC;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'dataset1_test', count(*)
FROM dataset1_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'refh_tmp_test', count(*)
FROM refh_tmp_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'indemnisation_test', count(*)
FROM indemnisation_test;

DROP TABLE ${hiveconf:SCHEMA}.dataset1_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset2_test;
DROP TABLE ${hiveconf:SCHEMA}.indemnisation_test;
DROP TABLE ${hiveconf:SCHEMA}.indemnisation_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.refh_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.ddi_tmp_test;
