/************* SRE-V2 DATASET TEST STEP 3
***************/

set hiveconf: SCHEMA = fb00_uda03423;
set SCHEMA;

set hiveconf: test_start = '2017-07-01';
set hiveconf: test_end = '2017-10-01';

set test_start;
set test_end;

-- Find rsa data in pr00_ppx005_dcores_minimasociaux

-- 1) inner join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.rsa_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.rsa_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
E.kc_statusminimasociaux_id as rsa,
E.kc_filiereminimasociaux_id,
E.kd_datemodification
FROM ${hiveconf:SCHEMA}.dataset5_test B
INNER JOIN braff00.pr00_ppx005_dcores_minimasociaux E
WHERE (B.ident = E.kc_individu_local
    AND E.kd_datemodification <= TO_DATE(${hiveconf:test_end}))
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.rsa_tmp2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.rsa_tmp2_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.rsa
FROM (SELECT *, RANK () OVER (
        PARTITION BY ident ORDER BY kd_datemodification DESC,
        kc_filiereminimasociaux_id DESC, rsa DESC) AS rk FROM
    ${hiveconf:SCHEMA}.rsa_tmp_test) B
WHERE B.rk = 1
;


-- 2) left join
DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset6_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset6_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
E.rsa
FROM ${hiveconf:SCHEMA}.dataset5_test B
LEFT JOIN ${hiveconf:SCHEMA}.rsa_tmp2_test E
ON B.ident = E.ident
;


-- Find dpae data

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dpae_tmp_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dpae_tmp_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
C.kd_dateembauche,
C.kd_datecreation,
C.dc_typecontrat_id,
C.dd_datefincdd
FROM ${hiveconf:SCHEMA}.dataset6_test B
INNER JOIN braff00.pr00_ppx005_xdpdpa_dpae C
ON B.ident = C.dc_individu_local
AND B.bni = C.dc_individu_national
WHERE C.kd_datecreation < TO_DATE(${hiveconf:test_start})
AND C.dc_typecontrat_id IS NOT NULL
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dpae_tmp2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dpae_tmp2_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
C.dpae_count,
D.dc_typecontrat_id AS dpae_last_id,
DATEDIFF(D.dd_datefincdd, D.kd_dateembauche) AS delta_cdd
FROM ${hiveconf:SCHEMA}.dpae_tmp_test B
INNER JOIN (SELECT ident,
    COUNT(kd_dateembauche) AS dpae_count
    FROM ${hiveconf:SCHEMA}.dpae_tmp_test
    GROUP BY ident) C
ON B.ident = C.ident
INNER JOIN (SELECT *, RANK() OVER(
        PARTITION BY ident ORDER BY kd_dateembauche DESC, kd_datecreation DESC) as rk FROM
    ${hiveconf:SCHEMA}.dpae_tmp_test) D
ON B.ident = D.ident
WHERE D.rk = 1
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset7_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset7_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
B.rsa,
E.dpae_count,
E.dpae_last_id,
E.delta_cdd
FROM ${hiveconf:SCHEMA}.dataset6_test B
LEFT JOIN ${hiveconf:SCHEMA}.dpae_tmp2_test E
ON B.ident = E.ident
;

-- Find past ict1 data

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.ict1_tmp1_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.ict1_tmp1_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
B.dn_valeurindicateur as ict1,
B.kn_annee as year,
B.kn_moisdebut as month_start,
B.dn_moisfin as month_end
FROM braff00.pr00_psd011_indicateurict1 B
INNER JOIN ${hiveconf:SCHEMA}.dataset7_test A
ON A.ident = B.dc_individu_local
AND A.bni = B.kn_individu_national
WHERE TO_DATE(CONCAT(B.kn_annee, '-', B.kn_moisdebut, '-01')) >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 365)
AND TO_DATE(CONCAT(B.kn_annee, '-', B.dn_moisfin, '-01')) < DATE_SUB(TO_DATE(${hiveconf:test_start}), 120)
;


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.ict1_tmp2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.ict1_tmp2_test
STORED AS ORC
AS SELECT
ident,
SUM(ict1) as ict1_count 
FROM ${hiveconf:SCHEMA}.ict1_tmp1_test 
GROUP BY ident
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset8_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset8_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
B.rsa,
B.dpae_count,
B.dpae_last_id,
B.delta_cdd,
E.ict1_count
FROM ${hiveconf:SCHEMA}.dataset7_test B
LEFT JOIN ${hiveconf:SCHEMA}.ict1_tmp2_test E
ON B.ident = E.ident
;

-- Find contact data

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.contact_tmp1_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.contact_tmp1_test
STORED AS ORC
AS SELECT DISTINCT
B.ident,
B.bni,
C.kn_contact,
C.dc_senscontactde_id,
C.kd_datemodification
FROM ${hiveconf:SCHEMA}.dataset8_test B
INNER JOIN (SELECT * , RANK() OVER (
        PARTITION BY dc_individu_local, kn_contact ORDER BY kd_datemodification DESC) AS rk FROM
    braff00.pr00_ppx005_tcogci_contact) C
ON B.ident = C.dc_individu_local
AND B.bni = C.dn_individu_national
WHERE C.kd_datemodification < TO_DATE(${hiveconf:test_start})
AND C.dc_senscontactde_id="E"
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.contact_tmp2_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.contact_tmp2_test
STORED AS ORC
AS SELECT
A.ident,
A.three_months_contact
FROM (SELECT ident,
    COUNT(kn_contact) AS three_months_contact
    FROM ${hiveconf:SCHEMA}.contact_tmp1_test
    WHERE kd_datemodification >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 60)
    GROUP BY ident) A
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.contact_tmp3_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.contact_tmp3_test
STORED AS ORC
AS SELECT
A.ident,
A.six_months_contact
FROM (SELECT ident,
    COUNT(kn_contact) AS six_months_contact
    FROM ${hiveconf:SCHEMA}.contact_tmp1_test
    WHERE kd_datemodification >= DATE_SUB(TO_DATE(${hiveconf:test_start}), 180)
    GROUP BY ident) A
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset9_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset9_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
B.rsa,
B.dpae_count,
B.dpae_last_id,
B.delta_cdd,
B.ict1_count,
E.three_months_contact,
F.six_months_contact
FROM ${hiveconf:SCHEMA}.dataset8_test B
LEFT JOIN ${hiveconf:SCHEMA}.contact_tmp2_test E
ON B.ident = E.ident
LEFT JOIN ${hiveconf:SCHEMA}.contact_tmp3_test F
ON B.ident = F.ident
;

-- Find bassin and score_forma_diap

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset10_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset10_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
B.rsa,
B.dpae_count,
B.dpae_last_id,
B.delta_cdd,
B.ict1_count,
B.three_months_contact,
B.six_months_contact,
E.bassin
FROM ${hiveconf:SCHEMA}.dataset9_test B
LEFT JOIN ${hiveconf:SCHEMA}.bassin E
ON B.depcom = E.depcom
;


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset11_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset11_test
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
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav,
B.rsa,
B.dpae_count,
B.dpae_last_id,
B.delta_cdd,
B.ict1_count,
B.three_months_contact,
B.six_months_contact,
B.bassin,
ROUND(E.score_forma_diag) as score_forma_diag
FROM ${hiveconf:SCHEMA}.dataset10_test B
LEFT JOIN ${hiveconf:SCHEMA}.score_forma_diag E
ON (B.bassin = E.bassin
    AND B.rome = E.rome)
;


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset12_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset12_test
STORED AS ORC
AS SELECT DISTINCT
*
FROM ${hiveconf:SCHEMA}.dataset11_test
WHERE (rome IS NOT NULL
    AND bassin IS NOT NULL
    AND ale IS NOT NULL
    AND motins IS NOT NULL
    AND depcom IS NOT NULL
    )
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.pr00_ppx005_ddi_priseencharge_tmp;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.pr00_ppx005_ddi_priseencharge_tmp
STORED AS ORC
AS SELECT
*
FROM braff00.pr00_ppx005_ddi_priseencharge
;

/*** DATA SUPERVISION ***/

use fb00_uda03423;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'rsa_tmp2_test', count(*)
FROM rsa_tmp2_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'dpae_tmp2_test', count(*)
FROM dpae_tmp2_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'ict1_tmp2_test', count(*)
FROM ict1_tmp2_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'contact_tmp2_test', count(*)
FROM contact_tmp2_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'dataset12_test', count(*)
FROM dataset12_test;


DROP TABLE ${hiveconf:SCHEMA}.dataset5_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset6_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset7_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset8_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset9_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset10_test;
DROP TABLE ${hiveconf:SCHEMA}.dataset11_test;
DROP TABLE ${hiveconf:SCHEMA}.contact_tmp1_test;
DROP TABLE ${hiveconf:SCHEMA}.contact_tmp2_test;
DROP TABLE ${hiveconf:SCHEMA}.contact_tmp3_test;
DROP TABLE ${hiveconf:SCHEMA}.contact_tmp4_test;
DROP TABLE ${hiveconf:SCHEMA}.rsa_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.rsa_tmp2_test;
