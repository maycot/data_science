/************* SRE-V2 DATASET TEST STEP 4
***************/

set hiveconf: SCHEMA = fb00_uda03423;
set SCHEMA;

set hiveconf: SCHEMA_HBASE = fb00_uda03400;
set SCHEMA_HBASE;

set hiveconf: test_start = '2017-07-01';
set hiveconf: test_end = '2017-10-01';

set test_start;
set test_end;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset13_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset13_test
STORED AS ORC
AS SELECT
A.*,
YEAR(TO_DATE(${hiveconf:test_end})) - YEAR(A.datnais) AS age,
SUBSTRING(A.rome, 1, 3) AS domaine_pro,
CAST(A.mobdist AS INT) AS mobdist_int,
CAST(NVL(A.qualif, 0) AS INT) AS qual,
CAST(NVL(A.exper, 0) AS INT) AS exper_int,
CAST(SUBSTRING(A.salmt, 1, 6) AS INT) AS salmt_int,
CASE WHEN montant_indem > 50000 THEN 50000
     WHEN montant_indem IS NULL THEN 0
     ELSE montant_indem END AS montant_indem_tmp,
CASE WHEN duree_indem > 1000 THEN 1000
     WHEN duree_indem IS NULL THEN 0
     ELSE duree_indem END AS duree_indem_tmp,
CASE WHEN motins = 11 THEN 11
     WHEN motins = 12 OR motins = 17 OR motins = 26 THEN 12
     WHEN motins = 13 OR motins = 27 THEN 13
     WHEN motins = 14 OR motins = 16 OR motins = 18 THEN 14
     WHEN (motins = 19 OR motins = 20 OR motins = 23  OR motins = 31
           OR motins = 33 OR motins = 34 OR motins = 36 OR motins = 38) THEN 19
     WHEN motins = 21 OR motins = 24 THEN 21
     WHEN motins = 22 THEN 22
     WHEN motins = 25 THEN 25
     WHEN motins = 28 THEN 28
     WHEN motins = 29 THEN 29
     WHEN motins = 30 THEN 30
     WHEN motins = 35 THEN 35
     WHEN motins = 37 THEN 37
     WHEN motins = 39 THEN 39
     ELSE 0 END AS motins_categ,     
DATEDIFF(TO_DATE(date_fin_indem), TO_DATE(${hiveconf:test_start})) AS duree_reste_indem,
CAST(ROUND(NVL(A.three_months_h_trav, 0)) AS INT) AS three_months_h_trav_int,
CAST(ROUND(NVL(A.three_months_s_trav, 0)) AS INT) AS three_months_s_trav_int,
CAST(ROUND(NVL(A.six_months_h_trav, 0)) AS INT) AS six_months_h_trav_int,
CAST(ROUND(NVL(A.six_months_s_trav, 0)) AS INT) AS six_months_s_trav_int,
ROUND(NVL(score_forma_diag, 0)) AS score_forma_diag_round,
CASE WHEN SUBSTRING(A.depcom, 1, 2) = "97" THEN SUBSTRING(A.depcom, 1, 3)
     ELSE SUBSTRING(A.depcom, 1, 2) END AS dep_tmp
FROM ${hiveconf:SCHEMA}.dataset12_test A
WHERE SUBSTRING(A.rome, 1, 3) != "B17"
AND SUBSTRING(A.depcom, 1, 2) != "98"
AND SUBSTRING(A.depcom, 1, 2) != "99"
;


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset14_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset14_test
STORED AS ORC
AS SELECT DISTINCT
ident,
bni,
bassin,
ale,
motins_categ as motins,
age,
rome,
domaine_pro,
sexe,
contrat,
duree_indem_tmp as duree_indem,
duree_reste_indem,
qual,
three_months_contact,
six_months_contact,
score_forma_diag_round as score_forma_diag,
CASE WHEN three_months_s_trav_int = 0 THEN 0
     WHEN three_months_s_trav_int > 0 AND three_months_s_trav_int <= 948 THEN 1
     WHEN three_months_s_trav_int > 948 AND three_months_s_trav_int <= 1700 THEN 2
     WHEN three_months_s_trav_int > 1700 AND three_months_s_trav_int <= 2800 THEN 3
     ELSE 4 END AS three_months_s_trav_categ, 
CASE WHEN three_months_h_trav_int = 0 THEN 0
     WHEN three_months_h_trav_int > 0 AND three_months_h_trav_int <= 87 THEN 1
     WHEN three_months_h_trav_int > 87 AND three_months_h_trav_int <= 162 THEN 2
     WHEN three_months_h_trav_int > 162 AND three_months_h_trav_int <= 263 THEN 3
     ELSE 4 END AS three_months_h_trav_categ, 
CASE WHEN six_months_s_trav_int = 0 THEN 0
     WHEN six_months_s_trav_int > 0 AND six_months_s_trav_int <= 1600 THEN 1
     WHEN six_months_s_trav_int > 1600 AND six_months_s_trav_int <= 3700 THEN 2
     WHEN six_months_s_trav_int > 3700 AND six_months_s_trav_int <= 6200 THEN 3
     ELSE 4 END AS six_months_s_trav_categ, 
CASE WHEN six_months_h_trav_int = 0 THEN 0
     WHEN six_months_h_trav_int > 0 AND six_months_h_trav_int <= 153 THEN 1
     WHEN six_months_h_trav_int > 153 AND six_months_h_trav_int <= 360 THEN 2
     WHEN six_months_h_trav_int > 360 AND six_months_h_trav_int <= 568 THEN 3
     ELSE 4 END AS six_months_h_trav_categ, 
CASE WHEN age < 23 THEN 0
     WHEN age >= 23 AND age < 33 THEN 1
     WHEN age >= 33 AND age < 50 THEN 2
     ELSE 3 END AS age_categ, 
CASE WHEN mobunit = "MN" AND mobdist_int >= 60 THEN 2
     WHEN mobunit = "H" AND mobdist_int >= 1 THEN 2
     WHEN mobunit = "KM" AND mobdist_int > 39 THEN 2
     WHEN mobunit = "MN" AND mobdist_int >= 30 AND mobdist_int < 60 THEN 1
     WHEN mobunit = "H" AND mobdist_int < 1 AND mobdist_int > 0 THEN 1
     WHEN mobunit = "KM" AND mobdist_int > 14 AND mobdist_int <= 39 THEN 1
     ELSE 0 END AS mobil,
CASE WHEN rsa IS NOT NULL THEN 1 ELSE 0 END AS benefrsa,
CASE WHEN nivfor = "AFS" THEN 1
     WHEN nivfor = "C12" THEN 2
     WHEN nivfor = "C3A" THEN 2
     WHEN nivfor = "CFG" THEN 2
     WHEN nivfor = "CP4" THEN 2
     WHEN nivfor = "NV5" THEN 3
     WHEN nivfor = "NV4" THEN 4
     WHEN nivfor = "NV3" THEN 5
     WHEN nivfor = "NV2" THEN 6
     WHEN nivfor = "NV1" THEN 7
     ELSE 3 END AS form, 
CASE WHEN obligemc IS NOT NULL THEN 1 ELSE 0 END AS th,
CASE WHEN exper_int = 0 THEN 0
     WHEN exper_int > 0 AND exper_int <= 2 THEN 1
     WHEN exper_int > 2 AND exper_int <= 5 THEN 2
     WHEN exper_int > 5 AND exper_int <= 10 THEN 3
     ELSE 4 END AS exper_classe,
CASE WHEN temps = "1" THEN 1 ELSE 0 END AS temps_plein,
CASE WHEN nb_enf = "00" THEN 0 ELSE 1 END AS nenf,
CASE WHEN salunit = "H" THEN salmt_int * 40 * 4 * 12
     WHEN salunit = "M" THEN salmt_int * 12
     ELSE 0 END AS salaire_tmp,
CASE WHEN qpv IS NOT NULL THEN 1 ELSE 0 END AS resqpv,
CASE WHEN diplome = "D" THEN 1 ELSE 0 END AS dipl,
CASE WHEN sitmat = "C" THEN 1
     WHEN sitmat = "V" THEN 1
     WHEN sitmat = "D" THEN 2
     WHEN sitmat = "M" THEN 3
     ELSE 1 END AS matrimon,
CASE WHEN axe_trav = "F" THEN 1
     WHEN axe_trav = "M" THEN 2
     ELSE 0 END AS axetrav,
CASE WHEN conssms = "N" THEN 1
     ELSE 0 END AS sms,
CASE WHEN consmail = "N" THEN 1
     ELSE 0 END AS mail,
CASE WHEN ctp IS NOT NULL THEN 1
     ELSE 0 END AS isctp,
CASE WHEN entrep IS NOT NULL THEN 1
     ELSE 0 END AS isentrep,
CASE WHEN dep_tmp = "2A" THEN "20"
     WHEN dep_tmp = "2B" THEN "20"
     ELSE dep_tmp END AS dep,
CASE WHEN montant_indem_tmp < 7400 THEN 1
     WHEN montant_indem_tmp >= 7400 AND montant_indem_tmp < 15600 THEN 2
     WHEN montant_indem_tmp >= 15600 AND montant_indem_tmp < 25500 THEN 3
     WHEN montant_indem_tmp >= 25500 THEN 4
     ELSE 0 END AS montant_indem_q,
CASE WHEN duree_indem_tmp > 0.01 THEN ROUND(montant_indem_tmp / duree_indem_tmp)
     ELSE 0 END AS montant_indem_j_tmp,
CASE WHEN sexe = "F" THEN 0
     ELSE 1 END AS sexe_tmp,
CASE WHEN contrat = 1 THEN 1
     WHEN contrat = 2 THEN 2
     WHEN contrat = 3 THEN 3
     ELSE 1 END AS contrat_tmp,
CASE WHEN dpae_count IS NULL THEN 0
     WHEN dpae_count = 1 THEN 1
     WHEN dpae_count > 1 AND dpae_count <= 5 THEN 2
     WHEN dpae_count > 5 AND dpae_count <= 20 THEN 3
     WHEN dpae_count > 20 THEN 4 END AS dpae_counter,
CASE WHEN dpae_last_id = "2" THEN 6
     WHEN dpae_last_id = "1" AND delta_cdd > 180 THEN 5
     WHEN dpae_last_id = "1" AND delta_cdd > 90 AND delta_cdd <= 180 THEN 4
     WHEN dpae_last_id = "1" AND delta_cdd > 30 AND delta_cdd <= 90 THEN 3
     WHEN dpae_last_id = "1" AND delta_cdd <= 30 THEN 2
     WHEN dpae_last_id = "1" AND delta_cdd IS NULL THEN 2
     WHEN dpae_last_id = "3" THEN 1
     ELSE 0 END AS last_dpae_type,
CASE WHEN ict1_count = 0 THEN 0
     ELSE 1 END AS has_ict1
FROM ${hiveconf:SCHEMA}.dataset13_test;


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset15_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset15_test
STORED AS ORC
AS SELECT DISTINCT
ident,
bni,
bassin,
ale,
motins,
age,
rome,
domaine_pro,
sexe_tmp as sexe,
contrat_tmp as contrat,
three_months_s_trav_categ,
six_months_s_trav_categ,
score_forma_diag,
duree_indem,
qual,
dep,
three_months_h_trav_categ,
six_months_h_trav_categ,
isentrep,
isctp,
montant_indem_q,
mail,
sms,
axetrav,
matrimon,
dipl,
resqpv,
nenf,
temps_plein,
exper_classe,
th,
form,
benefrsa,
mobil,
age_categ,
last_dpae_type,
has_ict1,
dpae_counter,
three_months_contact,
six_months_contact,
CASE WHEN salaire_tmp > 70000 THEN 70000
     ELSE salaire_tmp END AS salaire,
CASE WHEN montant_indem_j_tmp > 70 THEN 70
     ELSE montant_indem_j_tmp END AS montant_indem_j,
duree_reste_indem
FROM ${hiveconf:SCHEMA}.dataset14_test;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset16_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset16_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
A.bassin,
A.ale,
A.motins,
A.age,
A.rome,
A.domaine_pro,
A.sexe,
A.contrat,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
A.dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.last_dpae_type,
A.dpae_counter,
A.has_ict1,
A.three_months_contact,
A.six_months_contact,
A.salaire,
A.montant_indem_j,
NVL((A.montant_indem_j * A.duree_reste_indem), 0) AS montant_reste_indem,
B.nreg
FROM ${hiveconf:SCHEMA}.dataset15_test A
INNER JOIN ${hiveconf:SCHEMA}.correspondance_nreg_dep B
ON SUBSTRING(A.dep, 1, 2) = B.dep;

-- Tables de correspondances

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset17_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset17_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
B.bassin_emb as bassin,
G.ale_emb as ale,
C.motins_emb as motins,
A.age,
D.rome_emb as rome,
E.domaine_pro_emb as domaine_pro,
A.sexe,
A.contrat,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
H.dep_emb as dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.salaire,
A.has_ict1,
A.last_dpae_type,
A.three_months_contact,
A.six_months_contact,
A.dpae_counter,
A.montant_indem_j,
A.montant_reste_indem,
F.nreg_emb as nreg
FROM ${hiveconf:SCHEMA}.dataset16_test A
INNER JOIN ${hiveconf:SCHEMA}.encode_bassin B
ON A.bassin = B.bassin
INNER JOIN ${hiveconf:SCHEMA}.encode_motins C
ON A.motins = C.motins
INNER JOIN ${hiveconf:SCHEMA}.encode_rome D
ON A.rome = D.rome
INNER JOIN ${hiveconf:SCHEMA}.encode_domaine_pro E
ON A.domaine_pro = E.domaine_pro
INNER JOIN ${hiveconf:SCHEMA}.encode_nreg F
ON A.nreg = F.nreg
INNER JOIN ${hiveconf:SCHEMA}.encode_ale G
ON A.ale = G.ale
INNER JOIN ${hiveconf:SCHEMA}.encode_dep H
ON A.dep = H.dep
;

-- Add pec data

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset18_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset18_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
A.bassin,
A.ale,
A.motins,
A.age,
A.rome,
A.domaine_pro,
A.sexe,
A.contrat,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
A.dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.salaire,
A.has_ict1,
A.last_dpae_type,
A.dpae_counter,
A.montant_indem_j,
A.montant_reste_indem,
A.three_months_contact,
A.six_months_contact,
A.nreg,
B.cat_reg AS cat_reg_tmp,
B.datins,
C.type1_delta1_pec_count,
C.type1_delta1_pec_days_count,
C.type2_delta1_pec_count,
C.type2_delta1_pec_days_count,
C.type3_delta1_pec_count,
C.type3_delta1_pec_days_count,
C.type4_delta1_pec_count,
C.type4_delta1_pec_days_count,
C.type1_delta2_pec_count,
C.type1_delta2_pec_days_count,
C.type2_delta2_pec_count,
C.type2_delta2_pec_days_count,
C.type3_delta2_pec_count,
C.type3_delta2_pec_days_count,
C.type4_delta2_pec_count,
C.type4_delta2_pec_days_count,
C.type1_delta3_pec_count,
C.type1_delta3_pec_days_count,
C.type2_delta3_pec_count,
C.type2_delta3_pec_days_count,
C.type3_delta3_pec_count,
C.type3_delta3_pec_days_count,
C.type4_delta3_pec_count,
C.type4_delta3_pec_days_count
FROM ${hiveconf:SCHEMA}.dataset17_test A
INNER JOIN ${hiveconf:SCHEMA}.pec_tmp7_test B
ON A.ident = B.ident
INNER JOIN ${hiveconf:SCHEMA}.pec_metrics_test C
ON A.ident = C.ident
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset19_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset19_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
A.datins,
A.bassin,
A.ale,
A.motins,
A.age,
A.rome,
A.domaine_pro,
A.sexe,
A.contrat,
CASE WHEN A.cat_reg_tmp = 11 THEN 1
     WHEN A.cat_reg_tmp = 12 THEN 2
     WHEN A.cat_reg_tmp = 13 THEN 3
     ELSE 4 END AS cat_reg,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
A.dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.salaire,
A.has_ict1,
A.last_dpae_type,
A.dpae_counter,
A.montant_indem_j,
A.montant_reste_indem,
A.three_months_contact,
A.six_months_contact,
A.nreg,
(A.type1_delta1_pec_days_count + A.type2_delta1_pec_days_count + A.type3_delta1_pec_days_count
    + A.type4_delta1_pec_days_count) AS delta1_pec_days_count,
(A.type1_delta1_pec_count + A.type2_delta1_pec_count + A.type3_delta1_pec_count
    + A.type4_delta1_pec_count) AS delta1_pec_count,
A.type1_delta1_pec_days_count,
A.type2_delta1_pec_days_count,
A.type3_delta1_pec_days_count,
A.type4_delta1_pec_days_count,
(A.type1_delta2_pec_days_count + A.type2_delta2_pec_days_count + A.type3_delta2_pec_days_count
    + A.type4_delta2_pec_days_count) AS delta2_pec_days_count,
(A.type1_delta2_pec_count + A.type2_delta2_pec_count + A.type3_delta2_pec_count
    + A.type4_delta2_pec_count) AS delta2_pec_count,
A.type1_delta2_pec_days_count,
A.type2_delta2_pec_days_count,
A.type3_delta2_pec_days_count,
A.type4_delta2_pec_days_count,
(A.type1_delta3_pec_days_count + A.type2_delta3_pec_days_count + A.type3_delta3_pec_days_count
    + A.type4_delta3_pec_days_count) AS delta3_pec_days_count,
(A.type1_delta3_pec_count + A.type2_delta3_pec_count + A.type3_delta3_pec_count
    + A.type4_delta3_pec_count) AS delta3_pec_count,
A.type1_delta3_pec_days_count,
A.type2_delta3_pec_days_count,
A.type3_delta3_pec_days_count,
A.type4_delta3_pec_days_count
FROM ${hiveconf:SCHEMA}.dataset18_test A
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset20_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset20_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
A.bassin,
A.ale,
A.motins,
A.age,
A.rome,
A.domaine_pro,
A.sexe,
A.datins,
A.contrat,
A.cat_reg,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
A.dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.salaire,
A.has_ict1,
A.last_dpae_type,
A.dpae_counter,
A.montant_indem_j,
A.montant_reste_indem,
NVL(A.three_months_contact, 0) AS three_months_contact,
NVL(ROUND(A.six_months_contact / A.delta1_pec_days_count, 2), 0) AS six_months_contact_ratio,
A.nreg,
CASE WHEN A.delta1_pec_days_count > 180 THEN 180
     ELSE A.delta1_pec_days_count END AS delta1_pec_days_count_tmp,
A.delta1_pec_count,
A.type1_delta1_pec_days_count,
A.type2_delta1_pec_days_count,
A.type3_delta1_pec_days_count,
A.type4_delta1_pec_days_count,
CASE WHEN A.delta2_pec_days_count > 365 THEN 365
     ELSE A.delta2_pec_days_count END AS delta2_pec_days_count_tmp,
A.delta2_pec_count,
A.type1_delta2_pec_days_count,
A.type2_delta2_pec_days_count,
A.type3_delta2_pec_days_count,
A.type4_delta2_pec_days_count,
CASE WHEN A.delta3_pec_days_count > 730 THEN 730
     ELSE A.delta3_pec_days_count END AS delta3_pec_days_count_tmp,
A.delta3_pec_count,
A.type1_delta3_pec_days_count,
A.type2_delta3_pec_days_count,
A.type3_delta3_pec_days_count,
A.type4_delta3_pec_days_count
FROM ${hiveconf:SCHEMA}.dataset19_test A
WHERE salaire IS NOT NULL
;

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset21_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset21_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
CASE WHEN (A.datins >= TO_DATE(${hiveconf:test_start})
           AND A.datins <= TO_DATE(${hiveconf:test_end})) THEN MONTH(A.datins)
     ELSE 0 END AS month,
CASE WHEN (A.datins >= TO_DATE(${hiveconf:test_start}) 
           AND A.datins <= TO_DATE(${hiveconf:test_end})
           AND A.delta1_pec_days_count_tmp < 60) THEN 1
     ELSE 2 END AS type_pop2,
A.bassin,
A.ale,
A.motins,
A.age,
A.rome,
A.domaine_pro,
A.sexe,
A.contrat,
A.cat_reg,
A.three_months_s_trav_categ,
A.six_months_s_trav_categ,
A.score_forma_diag,
A.duree_indem,
A.qual,
A.dep,
A.three_months_h_trav_categ,
A.six_months_h_trav_categ,
A.isentrep,
A.isctp,
A.montant_indem_q,
A.mail,
A.sms,
A.axetrav,
A.matrimon,
A.dipl,
A.resqpv,
A.nenf,
A.temps_plein,
A.exper_classe,
A.th,
A.form,
A.benefrsa,
A.mobil,
A.age_categ,
A.salaire,
CASE WHEN A.salaire > 1 AND A.salaire < 10000 THEN 2
     WHEN A.salaire > 10000 AND A.salaire <= 17000 THEN 3
     WHEN A.salaire > 17000 AND A.salaire <= 20000 THEN 4
     WHEN A.salaire > 20000 AND A.salaire <= 25000 THEN 5
     WHEN A.salaire > 25000 AND A.salaire <= 35000 THEN 6
     WHEN A.salaire > 35000 AND A.salaire <= 50000 THEN 7
     ELSE 8 END AS salaire_categ,
A.has_ict1,
A.last_dpae_type,
A.dpae_counter,
A.montant_indem_j,
A.montant_reste_indem,
CASE WHEN three_months_contact = 0 THEN 0
     WHEN three_months_contact > 0 AND three_months_contact < 2 THEN 1
     WHEN three_months_contact >= 2 AND three_months_contact < 4 THEN 2
     WHEN three_months_contact >= 4 AND three_months_contact <= 6 THEN 3
     ELSE 4 END AS three_months_contact_categ,
CASE WHEN six_months_contact_ratio = 0 THEN 0
     WHEN six_months_contact_ratio > 0 AND six_months_contact_ratio <= 0.05 THEN 1
     WHEN six_months_contact_ratio > 0.05 AND six_months_contact_ratio <= 0.07 THEN 2
     WHEN six_months_contact_ratio > 0.07 AND six_months_contact_ratio <= 0.2 THEN 3
     ELSE 4 END AS six_months_contact_ratio_categ,
A.nreg,
A.delta1_pec_days_count_tmp AS delta1_pec_days_count,
A.delta1_pec_count,
A.type1_delta1_pec_days_count,
A.type2_delta1_pec_days_count,
A.type3_delta1_pec_days_count,
A.type4_delta1_pec_days_count,
A.delta2_pec_days_count_tmp AS delta2_pec_days_count,
A.delta2_pec_count,
A.type1_delta2_pec_days_count,
A.type2_delta2_pec_days_count,
A.type3_delta2_pec_days_count,
A.type4_delta2_pec_days_count,
A.delta3_pec_days_count_tmp AS delta3_pec_days_count,
A.delta3_pec_count,
A.type1_delta3_pec_days_count,
A.type2_delta3_pec_days_count,
A.type3_delta3_pec_days_count,
A.type4_delta3_pec_days_count
FROM ${hiveconf:SCHEMA}.dataset20_test A
WHERE (A.cat_reg <> 4
    OR A.isctp = 1 OR A.isentrep = 1) 
;

-- Final dataset with normalized no_emb features

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset22_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset22_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
A.month,
A.nreg,
A.dep,
A.bassin,
A.domaine_pro,
A.rome,
A.motins,
A.type_pop2,
A.exper_classe,
A.form,
A.six_months_h_trav_categ,
A.age_categ,
A.qual,
A.salaire_categ,
A.montant_indem_q,
ROUND((A.six_months_s_trav_categ - B.six_months_s_trav_categ_mean) / B.six_months_s_trav_categ_std, 3) as six_months_s_trav_categ_n,
ROUND((A.salaire - B.salaire_mean) / B.salaire_std, 3) as salaire_n,
ROUND((A.dpae_counter - B.dpae_counter_mean) / B.dpae_counter_std, 3) as dpae_counter_n,
ROUND((A.delta3_pec_days_count - B.delta3_pec_days_count_mean) / B.delta3_pec_days_count_std, 3) as delta3_pec_days_count_n,
ROUND((A.cat_reg - B.cat_reg_mean) / B.cat_reg_std, 3) as cat_reg_n,
ROUND((A.age - B.age_mean) / B.age_std, 3) as age_n,
ROUND((A.contrat - B.contrat_mean) / B.contrat_std, 3) as contrat_n,
ROUND((A.duree_indem - B.duree_indem_mean) / B.duree_indem_std, 3) as duree_indem_n,
ROUND((A.six_months_contact_ratio_categ - B.six_months_contact_ratio_categ_mean) / B.six_months_contact_ratio_categ_std, 3) as six_months_contact_ratio_categ_n,
ROUND((A.last_dpae_type - B.last_dpae_type_mean) / B.last_dpae_type_std, 3) as last_dpae_type_n,
ROUND((A.delta3_pec_count - B.delta3_pec_count_mean) / B.delta3_pec_count_std, 3) as delta3_pec_count_n,
ROUND((A.three_months_contact_categ - B.three_months_contact_categ_mean) / B.three_months_contact_categ_std, 3) as three_months_contact_categ_n,
ROUND((A.delta1_pec_count - B.delta1_pec_count_mean) / B.delta1_pec_count_std, 3) as delta1_pec_count_n,
ROUND((A.delta1_pec_days_count - B.delta1_pec_days_count_mean) / B.delta1_pec_days_count_std, 3) as delta1_pec_days_count_n,
ROUND((A.temps_plein - B.temps_plein_mean) / B.temps_plein_std, 3) as temps_plein_n,
ROUND((A.dipl - B.dipl_mean) / B.dipl_std, 3) as dipl_n,
ROUND((A.mobil - B.mobil_mean) / B.mobil_std, 3) as mobil_n,
ROUND((A.has_ict1 - B.has_ict1_mean) / B.has_ict1_std, 3) as has_ict1_n,
ROUND((A.axetrav - B.axetrav_mean) / B.axetrav_std, 3) as axetrav_n,
ROUND((A.benefrsa - B.benefrsa_mean) / B.benefrsa_std, 3) as benefrsa_n,
ROUND((A.isentrep - B.isentrep_mean) / B.isentrep_std, 3) as isentrep_n,
ROUND((A.th - B.th_mean) / B.th_std, 3) as th_n,
ROUND((A.mail - B.mail_mean) / B.mail_std, 3) as mail_n,
ROUND((A.sms - B.sms_mean) / B.sms_std, 3) as sms_n,
ROUND((A.matrimon - B.matrimon_mean) / B.matrimon_std, 3) as matrimon_n,
ROUND((A.nenf - B.nenf_mean) / B.nenf_std, 3) as nenf_n,
ROUND((A.montant_indem_j - B.montant_indem_j_mean) / B.montant_indem_j_std, 3) as montant_indem_j_n,
ROUND((A.three_months_h_trav_categ - B.three_months_h_trav_categ_mean) / B.three_months_h_trav_categ_std, 3) as three_months_h_trav_categ_n,
ROUND((A.three_months_s_trav_categ - B.three_months_s_trav_categ_mean) / B.three_months_s_trav_categ_std, 3) as three_months_s_trav_categ_n,
ROUND((A.type1_delta1_pec_days_count - B.type1_delta1_pec_days_count_mean) / B.type1_delta1_pec_days_count_std, 3) as type1_delta1_pec_days_count_n,
ROUND((A.type2_delta1_pec_days_count - B.type2_delta1_pec_days_count_mean) / B.type2_delta1_pec_days_count_std, 3) as type2_delta1_pec_days_count_n,
ROUND((A.type3_delta1_pec_days_count - B.type3_delta1_pec_days_count_mean) / B.type3_delta1_pec_days_count_std, 3) as type3_delta1_pec_days_count_n,
ROUND((A.type4_delta1_pec_days_count - B.type4_delta1_pec_days_count_mean) / B.type4_delta1_pec_days_count_std, 3) as type4_delta1_pec_days_count_n,
ROUND((A.delta2_pec_count - B.delta2_pec_count_mean) / B.delta2_pec_count_std, 3) as delta2_pec_count_n,
ROUND((A.type1_delta3_pec_days_count - B.type1_delta3_pec_days_count_mean) / B.type1_delta3_pec_days_count_std, 3) as type1_delta3_pec_days_count_n,
ROUND((A.type2_delta3_pec_days_count - B.type2_delta3_pec_days_count_mean) / B.type2_delta3_pec_days_count_std, 3) as type2_delta3_pec_days_count_n,
ROUND((A.type3_delta3_pec_days_count - B.type3_delta3_pec_days_count_mean) / B.type3_delta3_pec_days_count_std, 3) as type3_delta3_pec_days_count_n,
ROUND((A.type4_delta3_pec_days_count - B.type4_delta3_pec_days_count_mean) / B.type4_delta3_pec_days_count_std, 3) as type4_delta3_pec_days_count_n,
ROUND((A.score_forma_diag - B.score_forma_diag_mean) / B.score_forma_diag_std, 3) as score_forma_diag_n
FROM ${hiveconf:SCHEMA}.dataset21_test A, ${hiveconf:SCHEMA}.train_metrics B; 


DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.dataset_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.dataset_test
STORED AS ORC
AS SELECT DISTINCT
ident,
bni,
month,
nreg,
dep,
bassin,
domaine_pro,
rome,
motins,
type_pop2,
exper_classe,
form,
six_months_h_trav_categ,
age_categ,
qual,
salaire_categ,
montant_indem_q,
six_months_s_trav_categ_n,
salaire_n,
dpae_counter_n,
delta3_pec_days_count_n,
cat_reg_n,
age_n,
contrat_n,
duree_indem_n,
six_months_contact_ratio_categ_n,
last_dpae_type_n,
delta3_pec_count_n,
three_months_contact_categ_n,
delta1_pec_count_n,
delta1_pec_days_count_n,
temps_plein_n,
dipl_n,
mobil_n,
has_ict1_n,
axetrav_n,
benefrsa_n,
isentrep_n,
th_n,
mail_n,
sms_n,
matrimon_n,
nenf_n,
montant_indem_j_n,
three_months_h_trav_categ_n,
three_months_s_trav_categ_n,
type1_delta1_pec_days_count_n,
type2_delta1_pec_days_count_n,
type3_delta1_pec_days_count_n,
type4_delta1_pec_days_count_n,
delta2_pec_count_n,
type1_delta3_pec_days_count_n,
type2_delta3_pec_days_count_n,
type3_delta3_pec_days_count_n,
type4_delta3_pec_days_count_n,
score_forma_diag_n
FROM ${hiveconf:SCHEMA}.dataset22_test
;

/*** DATA SUPERVISION ***/

use fb00_uda03423;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'dataset_test', count(*)
FROM dataset_test;

hive -e 'set hive.cli.print.header=true;select * from fb00_uda03423.dataset_test' | sed 's/[\t]/;/g' > /donapp/uda034/p00/dat/dataset_stock_test_7_9.csv

DROP TABLE  ${hiveconf:SCHEMA}.dataset12_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset13_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset14_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset15_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset16_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset17_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset18_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset19_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset20_test;
DROP TABLE  ${hiveconf:SCHEMA}.dataset21_test;
DROP TABLE  ${hiveconf:SCHEMA}.pec_tmp7_test;
