DROP TABLE IF EXISTS fb00_uda03430.pec_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec_tmp1
STORED AS ORC
AS SELECT DISTINCT
B.kn_individu_national,
A.kc_individu_local,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_structure,
A.kn_numpec,
A.dd_dateinscriptionpec,
A.dc_soustypepec,
A.dc_categoriede_id,
A.dc_motifinscription_id,
A.dc_situationanterieure_id
FROM fb00_uda03430.demandeurs_tmp3 B
INNER JOIN braff00.pr00_ppx005_ddiadh_priseencharge A
ON A.kn_individu_national = B.kn_individu_national
WHERE A.kd_datemodification <= '2021-12-20';

DROP TABLE IF EXISTS fb00_uda03430.pec_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_structure,
A.kn_numpec,
A.dd_dateinscriptionpec,
A.dc_soustypepec,
A.dc_categoriede_id,
A.dc_motifinscription_id,
A.dc_situationanterieure_id
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national, kn_numpec
    ORDER BY kd_datemodification DESC) AS rk FROM
    fb00_uda03430.pec_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.pec_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec_tmp3
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kd_datemodification,
A.dc_typepec_id,
A.dc_structure,
A.kn_numpec,
A.dd_dateinscriptionpec,
A.dc_soustypepec,
A.dc_categoriede_id,
A.dc_motifinscription_id,
A.dc_situationanterieure_id
FROM fb00_uda03430.pec_tmp1 A
INNER JOIN fb00_uda03430.demandeurs_tmp3 B
ON (A.kn_individu_national = B.kn_individu_national
AND A.kn_numpec = B.kn_numpec
AND A.kd_datemodification = B.kd_datemodification);

DROP TABLE IF EXISTS fb00_uda03430.pec_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec_tmp4
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.dc_typepec_id as previous_typepec,
A.dd_dateinscriptionpec as previous_dateinscriptionpec,
A.dc_soustypepec as previous_soustypepec,
A.dc_categoriede_id as previous_categoriede,
A.dc_motifinscription_id as previous_motifinscription
FROM (SELECT *, RANK () OVER (
    PARTITION BY kn_individu_national ORDER BY dd_dateinscriptionpec DESC,
    kn_numpec DESC) AS rk FROM fb00_uda03430.pec_tmp2) A
WHERE A.rk = 2;

DROP TABLE IF EXISTS fb00_uda03430.pec_count;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec_count
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT dc_typepec_id) as typepec_count,
COUNT(DISTINCT dc_structure) as ale_count,
COUNT(DISTINCT kn_numpec) as numpec_count
FROM fb00_uda03430.pec_tmp2
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.pec;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pec
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.dc_typepec_id,
A.dc_soustypepec,
DATEDIFF(A.dd_dateinscriptionpec, '2021-12-20') AS delta_dateinscriptionpec,
A.dc_categoriede_id,
A.dc_motifinscription_id,
A.dc_situationanterieure_id,
B.previous_typepec,
DATEDIFF(B.previous_dateinscriptionpec, '2021-12-20') AS delta_previous_dateinscriptionpec,
B.previous_soustypepec,
B.previous_categoriede,
B.previous_motifinscription,
C.typepec_count,
C.ale_count,
C.numpec_count
FROM fb00_uda03430.pec_tmp3 A
LEFT JOIN fb00_uda03430.pec_tmp4 B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN fb00_uda03430.pec_count C
ON A.kn_individu_national = c.kn_individu_national;


-- Données indemnisation : capital and duree

-- 1) inner join
DROP TABLE IF EXISTS fb00_uda03430.indemnisation_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.indemnisation_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
B.dn_capitalodcourante as montant_indem,
B.dn_dureedroitcourant as duree_indem,
B.dd_datetheoriquefindroit as date_fin_indem,
B.kd_datemodification,
B.kc_ouverturedroit,
B.kc_reprisedroit
FROM fb00_uda03430.demandeurs_tmp3 A
INNER JOIN braff00.pr00_ppx005_drvder_elementdroitncp B
ON A.kn_individu_national = B.kn_individu_national
WHERE TO_DATE(B.kd_datemodification) <= '2021-12-20';

DROP TABLE IF EXISTS fb00_uda03430.indemnisation_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.indemnisation_tmp2
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.montant_indem,
A.duree_indem,
A.date_fin_indem,
A.kc_ouverturedroit,
A.kc_reprisedroit,
A.kd_datemodification
FROM (SELECT *, RANK () OVER (
        PARTITION BY kn_individu_national, kc_ouverturedroit, kc_reprisedroit
        ORDER BY kd_datemodification DESC) AS rk FROM
    fb00_uda03430.indemnisation_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.indemnisation_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.indemnisation_tmp3
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT A.kc_ouverturedroit) as ouverturedroit_count,
COUNT(DISTINCT A.kc_reprisedroit) as reprisedroit_count
FROM fb00_uda03430.indemnisation_tmp2 A
GROUP BY A.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.indemnisation_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.indemnisation_tmp4
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.montant_indem,
A.duree_indem,
A.date_fin_indem
FROM (SELECT *, RANK () OVER (
        PARTITION BY kn_individu_national
        ORDER BY kd_datemodification DESC, montant_indem DESC) AS rk FROM
    fb00_uda03430.indemnisation_tmp2) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.indemnisation;
CREATE TABLE IF NOT EXISTS fb00_uda03430.indemnisation
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.montant_indem,
A.duree_indem,
A.date_fin_indem,
DATEDIFF(A.date_fin_indem, '2021-12-20') AS delta_fin_indem,
B.ouverturedroit_count,
B.reprisedroit_count
FROM fb00_uda03430.indemnisation_tmp4 A
INNER JOIN fb00_uda03430.indemnisation_tmp3 B
ON A.kn_individu_national = B.kn_individu_national;

-- Données de type ami entrants

-- Récupération des ami des bni
DROP TABLE IF EXISTS fb00_uda03430.ami_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.ami_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
B.kn_ami,
B.dd_datecreation
FROM fb00_uda03430.demandeurs_tmp3 A
INNER JOIN (SELECT * , RANK() OVER (
        PARTITION BY dn_individu_national, kn_ami
        ORDER BY kn_derniersuivi DESC) AS rk FROM
    braff00.pr00_ppx005_tinmer_ami) B
ON A.kn_individu_national = B.dn_individu_national
WHERE B.dd_datecreation < '2021-12-20';

-- Sélection des ami avec le premier échange en provenance du DE
DROP TABLE IF EXISTS fb00_uda03430.ami_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.ami_tmp2
STORED AS ORC
AS SELECT
A.kn_individu_national,
A.kn_ami
FROM fb00_uda03430.ami_tmp1 A
INNER JOIN (SELECT * , RANK() OVER (
        PARTITION BY dn_ami
        ORDER BY dn_numeroordre ASC) AS rk FROM
        braff00.pr00_ppx005_tinmer_lignesuiviami) B
ON A.kn_ami = B.dn_ami
WHERE B.rk = 1 AND B.dc_acteur_id = 'CT';

DROP TABLE IF EXISTS fb00_uda03430.ami;
CREATE TABLE IF NOT EXISTS fb00_uda03430.ami
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT A.kn_ami) AS ami_entrants_count
FROM fb00_uda03430.ami_tmp2 A
GROUP BY A.kn_individu_national;

-- Activité déclarée table actualisation

DROP TABLE IF EXISTS fb00_uda03430.actu_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.actu_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.kc_individu_local,
TO_DATE(CONCAT(20, B.kc_anneeactualisation , '-',
        B.kc_moisactualisation, '-', 01)) as dateactu,
(CASE WHEN B.dn_toptravail = '1' THEN B.dn_nbheuretravail ELSE 0 END) h_trav_m,
B.dn_salairebrut as s_trav_m,
B.dn_topstage,
B.dn_topmaladie,
B.dn_topmaternite,
B.dn_topretraite,
B.dn_topinvalidite,
B.dn_toprechercheemploi
FROM fb00_uda03430.pec_tmp2 A
INNER JOIN braff00.pr00_psigma_regimegeneralactu B
ON A.kc_individu_local = CONCAT(B.dc_assedic_id, B.kc_individu_local, B.dc_cleidentifiantindividu)
WHERE TO_DATE(CONCAT(20, B.kc_anneeactualisation , '-', B.kc_moisactualisation, '-00')) <= '2021-12-20'
AND TO_DATE(CONCAT(20, B.kc_anneeactualisation , '-', B.kc_moisactualisation, '-00')) >= DATE_SUB('2021-12-20', 180);

DROP TABLE IF EXISTS fb00_uda03430.actu_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.actu_tmp2
STORED AS ORC
AS SELECT
A.kn_individu_national,
A.kc_individu_local,
A.three_months_h_trav,
A.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav
FROM (SELECT kn_individu_national, kc_individu_local,
    SUM(h_trav_m) AS three_months_h_trav,
    SUM(s_trav_m) AS three_months_s_trav
    FROM fb00_uda03430.actu_tmp1
    WHERE dateactu >= DATE_SUB('2021-12-20', 90)
    GROUP BY kn_individu_national, kc_individu_local) A
INNER JOIN (SELECT kn_individu_national, kc_individu_local,
    SUM(h_trav_m) as six_months_h_trav,
    SUM(s_trav_m) as six_months_s_trav
    FROM fb00_uda03430.actu_tmp1
    WHERE dateactu >= DATE_SUB('2021-12-20', 180)
    GROUP BY kn_individu_national, kc_individu_local) B
ON (A.kc_individu_local = B.kc_individu_local
AND A.kn_individu_national = B.kn_individu_national);

DROP TABLE IF EXISTS fb00_uda03430.actu_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.actu_tmp3
STORED AS ORC
AS SELECT
A.kn_individu_national,
A.h_trav_m,
A.s_trav_m,
A.dn_topstage,
A.dn_topmaladie,
A.dn_topmaternite,
A.dn_topretraite,
A.dn_topinvalidite,
A.dn_toprechercheemploi
FROM (SELECT * , RANK() OVER (PARTITION BY kn_individu_national
        ORDER BY dateactu DESC) AS rk FROM fb00_uda03430.actu_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.actu;
CREATE TABLE IF NOT EXISTS fb00_uda03430.actu
STORED AS ORC
AS SELECT
A.kn_individu_national,
A.h_trav_m,
A.s_trav_m,
A.dn_topstage,
A.dn_topmaladie,
A.dn_topmaternite,
A.dn_topretraite,
A.dn_topinvalidite,
A.dn_toprechercheemploi,
B.three_months_h_trav,
B.three_months_s_trav,
B.six_months_h_trav,
B.six_months_s_trav
FROM fb00_uda03430.actu_tmp3 A
LEFT JOIN fb00_uda03430.actu_tmp2 B
ON A.kn_individu_national = B.kn_individu_national;

-- Periodes activité gaec

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
B.kn_periodeactivitegaec,
B.dd_datedebutactivite,
B.dd_datefinactivite,
B.dc_typeperiodegaec_id,
B.dc_origineinformationgaec_id,
B.dn_quantiteactivite,
B.dc_uniteactivite_id
FROM fb00_uda03430.pec_tmp2 A
INNER JOIN braff00.pr00_ppx005_drvder_periodeactivitegaec B
ON A.kn_individu_national = B.kn_individu_national
WHERE B.dd_datedebutactivite < '2021-12-20';

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp2
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT kn_periodeactivitegaec) AS periodeactgaec_total_count,
COUNT(DISTINCT dc_typeperiodegaec_id) AS typeperiodegaec_total_count,
COUNT(DISTINCT dc_origineinformationgaec_id) AS origineinformationgaec_total_count
FROM fb00_uda03430.gaec_tmp1
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp3
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT kn_periodeactivitegaec) AS three_months_periodeact_count,
SUM(dn_quantiteactivite) AS three_months_quantiteact_sum
FROM fb00_uda03430.gaec_tmp1 A
WHERE dd_datedebutactivite >= DATE_SUB('2021-12-20', 90)
AND dc_typeperiodegaec_id IN (15, 16, 17, 18, 2, 4, 7, 9)
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp4;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp4
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT kn_periodeactivitegaec) AS six_months_periodeact_count,
SUM(dn_quantiteactivite) AS six_months_quantiteact_sum
FROM fb00_uda03430.gaec_tmp1 A
WHERE dd_datedebutactivite >= DATE_SUB('2021-12-20', 180)
AND dc_typeperiodegaec_id IN (15, 16, 17, 18, 2, 4, 7, 9)
GROUP BY kn_individu_national;


DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp5;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp5
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT kn_periodeactivitegaec) AS six_months_evenmtperso_count,
SUM(dn_quantiteactivite) AS six_months_evenmtperso_sum
FROM fb00_uda03430.gaec_tmp1
WHERE dd_datedebutactivite >= DATE_SUB('2021-12-20', 180)
AND dc_typeperiodegaec_id IN (8, 10)
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp6;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp6
STORED AS ORC
AS SELECT
kn_individu_national,
COUNT(DISTINCT kn_periodeactivitegaec) AS six_months_maladie_count,
SUM(dn_quantiteactivite) AS six_months_maladie_sum
FROM fb00_uda03430.gaec_tmp1
WHERE dd_datedebutactivite >= DATE_SUB('2021-12-20', 180)
AND dc_typeperiodegaec_id = 13
GROUP BY kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.gaec_tmp7;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec_tmp7
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
DATEDIFF(A.dd_datefinactivite, '2021-12-20') AS delta_last_periodeact,
A.dc_typeperiodegaec_id AS last_typeperiodegaec,
A.dn_quantiteactivite AS last_quantiteactivite,
A.dc_uniteactivite_id AS last_uniteactivite
FROM (SELECT * , RANK() OVER (PARTITION BY kn_individu_national
        ORDER BY dd_datedebutactivite DESC, dd_datefinactivite DESC,
        kn_periodeactivitegaec DESC) AS rk
        FROM fb00_uda03430.gaec_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.gaec;
CREATE TABLE IF NOT EXISTS fb00_uda03430.gaec
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.delta_last_periodeact,
A.last_typeperiodegaec,
A.last_quantiteactivite,
A.last_uniteactivite,
B.periodeactgaec_total_count,
B.typeperiodegaec_total_count,
B.origineinformationgaec_total_count,
C.three_months_periodeact_count,
C.three_months_quantiteact_sum,
D.six_months_periodeact_count,
D.six_months_quantiteact_sum,
E.six_months_evenmtperso_count,
E.six_months_evenmtperso_sum,
F.six_months_maladie_count,
F.six_months_maladie_sum
FROM fb00_uda03430.gaec_tmp7 A
LEFT JOIN fb00_uda03430.gaec_tmp2 B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN fb00_uda03430.gaec_tmp3 C
ON A.kn_individu_national = C.kn_individu_national
LEFT JOIN fb00_uda03430.gaec_tmp4 D
ON A.kn_individu_national = D.kn_individu_national
LEFT JOIN fb00_uda03430.gaec_tmp5 E
ON A.kn_individu_national = E.kn_individu_national
LEFT JOIN fb00_uda03430.gaec_tmp6 F
ON A.kn_individu_national = F.kn_individu_national;

-- Données de type formation et diplôme

DROP TABLE IF EXISTS fb00_uda03430.dipl_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dipl_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
B.kn_idformation,
B.dc_domaineformation_id,
B.dc_typeformation_id,
B.dc_intitulediplome,
B.kd_datemodification
FROM fb00_uda03430.demandeurs_tmp3 A
INNER JOIN (SELECT * , RANK() OVER (
        PARTITION BY kn_individu_national, kn_idformation
        ORDER BY kd_datemodification DESC) AS rk FROM
    braff00.pr00_pdd014_formation) B
ON A.kn_individu_national = B.kn_individu_national
WHERE B.kd_datemodification < '2021-12-20'
AND B.dn_topobtentiondiplome = True
AND B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dipl_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dipl_tmp2
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT A.kn_idformation) AS forma_count,
COUNT(DISTINCT A.dc_domaineformation_id) AS domforma_count,
COUNT(DISTINCT A.dc_typeformation_id) AS typeforma_count
FROM fb00_uda03430.dipl_tmp1 A
GROUP BY A.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.dipl_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dipl_tmp3
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.dc_domaineformation_id AS last_domforma,
A.dc_typeformation_id AS last_typeforma,
A.dc_intitulediplome AS last_dipl
FROM (SELECT * , RANK() OVER (
        PARTITION BY kn_individu_national
        ORDER BY kd_datemodification DESC, kn_idformation DESC) AS rk FROM
    fb00_uda03430.dipl_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.dipl;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dipl
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.last_domforma,
A.last_typeforma,
A.last_dipl,
B.forma_count,
B.typeforma_count,
B.domforma_count
FROM fb00_uda03430.dipl_tmp3 A
LEFT JOIN fb00_uda03430.dipl_tmp2 B
ON A.kn_individu_national = B.kn_individu_national;

-- Données de type profil professionnel

DROP TABLE IF EXISTS fb00_uda03430.dcopro_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dcopro_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
B.dc_romev3_1_id as rome_profil,
B.dc_tempstravail_id as temps,
B.dc_situationfamille_id as sitmat,
B.dc_distanceoudureedeplacement as mobdist,
B.dc_unitemesuredeplacement as mobunit,
B.dc_nbreenfantacharge as nb_enf,
B.dc_qualificationemploi_1 as qualif,
B.dc_nivformation_id as nivfor,
B.kd_datemodification,
B.dc_axetravail_id,
B.dc_axetravailprincipal_id,
B.kc_profilde
FROM fb00_uda03430.demandeurs_tmp3 A
INNER JOIN braff00.pr00_ppx005_dcopro_profilprofessionnel B
ON A.kn_individu_national = B.kn_individu_national
WHERE B.kd_datemodification <= '2021-12-20'
AND B.dc_romev3_1_id IS NOT NULL;

DROP TABLE IF EXISTS fb00_uda03430.dcopro_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dcopro_tmp2
STORED AS ORC
AS SELECT
A.kn_individu_national,
COUNT(DISTINCT A.rome_profil) AS rome_profil_count
FROM fb00_uda03430.dcopro_tmp1 A
GROUP BY A.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.dcopro_tmp3;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dcopro_tmp3
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.rome_profil,
A.temps,
A.sitmat,
A.mobdist,
A.mobunit,
A.qualif,
A.nivfor,
A.nb_enf,
A.dc_axetravail_id,
A.dc_axetravailprincipal_id
FROM (SELECT *, RANK () OVER (
        PARTITION BY kn_individu_national
        ORDER BY kd_datemodification DESC, kc_profilde DESC) AS rk FROM
    fb00_uda03430.dcopro_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.features_rome_proche_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_rome_proche_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.rome2
FROM fb00_uda03430.rome_proche A
WHERE A.rome1 = 'D1106';

DROP TABLE IF EXISTS fb00_uda03430.features_rome_proche;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_rome_proche
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
1 as has_rome_proche
FROM fb00_uda03430.dcopro_tmp3 A
INNER JOIN fb00_uda03430.features_rome_proche_tmp1 B
ON A.rome_profil = B.rome2;

DROP TABLE IF EXISTS fb00_uda03430.dcopro;
CREATE TABLE IF NOT EXISTS fb00_uda03430.dcopro
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.rome_profil,
A.temps,
A.sitmat,
A.mobdist,
A.mobunit,
A.nb_enf,
A.qualif,
A.nivfor,
A.dc_axetravail_id,
A.dc_axetravailprincipal_id,
B.rome_profil_count,
C.has_rome_proche
FROM fb00_uda03430.dcopro_tmp3 A
INNER JOIN fb00_uda03430.dcopro_tmp2 B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN fb00_uda03430.features_rome_proche C
ON A.kn_individu_national = C.kn_individu_national;

-- Features

DROP TABLE IF EXISTS fb00_uda03430.features_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.dep,
G.had_pcs,
G.had_same_pcs,
G.had_pcs_proche,
G.pcs_counter,
G.naf_counter,
G.last_pcs,
G.last_naf_lieutrav,
G.last_naf_affect,
G.etab_dep,
G.residence_dep,
B.dc_typepec_id,
B.dc_soustypepec,
B.delta_dateinscriptionpec,
B.dc_categoriede_id,
B.dc_motifinscription_id,
B.dc_situationanterieure_id,
B.previous_typepec,
B.delta_previous_dateinscriptionpec,
B.previous_soustypepec,
B.previous_categoriede,
B.previous_motifinscription,
B.typepec_count,
B.ale_count,
B.numpec_count,
C.montant_indem,
C.duree_indem,
C.delta_fin_indem,
C.ouverturedroit_count,
C.reprisedroit_count,
D.rome_profil,
D.temps,
D.sitmat,
D.mobdist,
D.mobunit,
D.nb_enf,
D.qualif,
D.nivfor,
D.rome_profil_count,
D.has_rome_proche,
D.dc_axetravail_id,
D.dc_axetravailprincipal_id,
E.ami_entrants_count,
--F.label,
-- H.will_pcs,
-- H.will_pcs_proche,
I.h_trav_m,
I.s_trav_m,
I.dn_topstage,
I.dn_topmaladie,
I.dn_topmaternite,
I.dn_topretraite,
I.dn_topinvalidite,
I.dn_toprechercheemploi,
I.three_months_h_trav,
I.three_months_s_trav,
I.six_months_h_trav,
I.six_months_s_trav,
J.delta_last_periodeact,
J.last_typeperiodegaec,
J.last_quantiteactivite,
J.last_uniteactivite,
J.periodeactgaec_total_count,
J.typeperiodegaec_total_count,
J.origineinformationgaec_total_count,
J.three_months_periodeact_count,
J.three_months_quantiteact_sum,
J.six_months_periodeact_count,
J.six_months_quantiteact_sum,
J.six_months_evenmtperso_count,
J.six_months_evenmtperso_sum,
J.six_months_maladie_count,
J.six_months_maladie_sum,
K.last_domforma,
K.last_typeforma,
K.last_dipl,
K.forma_count,
K.typeforma_count,
K.domforma_count
FROM fb00_uda03430.demandeurs_tmp3 A
LEFT JOIN fb00_uda03430.dsn_prealable_tmp7 G
ON A.kn_individu_national = G.kn_individu_national
LEFT JOIN fb00_uda03430.pec B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN fb00_uda03430.indemnisation C
ON A.kn_individu_national = C.kn_individu_national
LEFT JOIN fb00_uda03430.dcopro D
ON A.kn_individu_national = D.kn_individu_national
LEFT JOIN fb00_uda03430.ami E
ON A.kn_individu_national = E.kn_individu_national
-- LEFT JOIN fb00_uda03430.label F
-- ON A.kn_individu_national = F.kn_individu_national
-- LEFT JOIN fb00_uda03430.dsn_ulterieure_tmp7 H
-- ON A.kn_individu_national = H.kn_individu_national
LEFT JOIN fb00_uda03430.actu I
ON A.kn_individu_national = I.kn_individu_national
LEFT JOIN fb00_uda03430.gaec J
ON A.kn_individu_national = J.kn_individu_national
LEFT JOIN fb00_uda03430.dipl K
ON A.kn_individu_national = K.kn_individu_national;

DROP TABLE IF EXISTS fb00_uda03430.features;
CREATE TABLE IF NOT EXISTS fb00_uda03430.features
STORED AS ORC
AS SELECT DISTINCT
A.kn_individu_national,
A.dep,
A.had_pcs,
A.had_same_pcs,
A.had_pcs_proche,
A.pcs_counter,
A.naf_counter,
A.last_pcs,
A.last_naf_lieutrav,
A.last_naf_affect,
A.etab_dep,
A.residence_dep,
A.dc_typepec_id,
A.dc_soustypepec,
A.delta_dateinscriptionpec,
A.dc_categoriede_id,
A.dc_motifinscription_id,
A.dc_situationanterieure_id,
A.previous_typepec,
A.delta_previous_dateinscriptionpec,
A.previous_soustypepec,
A.previous_categoriede,
A.previous_motifinscription,
A.typepec_count,
A.ale_count,
A.numpec_count,
A.montant_indem,
A.duree_indem,
A.delta_fin_indem,
A.ouverturedroit_count,
A.reprisedroit_count,
A.rome_profil,
A.temps,
A.sitmat,
A.mobdist,
A.mobunit,
A.nb_enf,
A.qualif,
A.nivfor,
A.rome_profil_count,
A.has_rome_proche,
A.dc_axetravail_id,
A.dc_axetravailprincipal_id,
A.ami_entrants_count,
--A.label,
-- A.will_pcs,
-- A.will_pcs_proche,
A.h_trav_m,
A.s_trav_m,
A.dn_topstage,
A.dn_topmaladie,
A.dn_topmaternite,
A.dn_topretraite,
A.dn_topinvalidite,
A.dn_toprechercheemploi,
A.three_months_h_trav,
A.three_months_s_trav,
A.six_months_h_trav,
A.six_months_s_trav,
A.delta_last_periodeact,
A.last_typeperiodegaec,
A.last_quantiteactivite,
A.last_uniteactivite,
A.periodeactgaec_total_count,
A.typeperiodegaec_total_count,
A.origineinformationgaec_total_count,
A.three_months_periodeact_count,
A.three_months_quantiteact_sum,
A.six_months_periodeact_count,
A.six_months_quantiteact_sum,
A.six_months_evenmtperso_count,
A.six_months_evenmtperso_sum,
A.six_months_maladie_count,
A.six_months_maladie_sum,
A.last_domforma,
A.last_typeforma,
A.last_dipl,
A.forma_count,
A.typeforma_count,
A.domforma_count,
B.dc_structure as ale,
B.dd_dateinscriptionpec as datins,
C.dc_nompatronymique as nom,
C.dc_prenom as prenom,
C.dc_sexe_id as sexe,
C.dd_datenaissance as datenais,
D.dc_specificiterome1,
D.dc_specificiterome2,
D.dc_specificiterome3,
D.dc_specificiterome4,
D.dc_specificiterome5,
D.dc_specificiterome6,
D.dc_specificiterome7,
D.dc_specificiterome8,
D.dc_specificiterome9,
D.dc_specificiterome10,
D.dc_specificiterome11,
D.dc_specificiterome12,
D.dc_specificiterome13,
D.dc_specificiterome14,
D.dc_specificiterome15,
D.dc_specificiterome16,
D.dc_specificiterome17,
D.dc_specificiterome18,
D.dc_specificiterome19,
D.dc_specificiterome20,
D.dc_specificiterome21,
D.dc_specificiterome22,
D.dc_specificiterome23,
D.dc_specificiterome24,
D.dc_specificiterome25,
D.dc_specificiterome26,
D.dc_specificiterome27,
D.dc_specificiterome28,
D.dc_specificiterome29,
D.dc_specificiterome30,
D.dc_specificiterome31,
D.dc_specificiterome32,
D.dc_specificiterome33,
D.dc_specificiterome34,
D.dc_specificiterome35,
D.dc_specificiterome36,
D.dc_specificiterome37,
D.dc_specificiterome38,
D.dc_specificiterome39,
D.dc_specificiterome40,
D.dc_specificiterome41,
D.dc_specificiterome42,
D.dc_specificiterome43,
D.dc_specificiterome44,
D.dc_specificiterome45,
D.dc_specificiterome46,
D.dc_specificiterome47,
D.dc_specificiterome48,
D.dc_specificiterome49,
D.dc_specificiterome50,
D.dc_specificiterome51,
D.dc_specificiterome52,
D.dc_specificiterome53,
D.dc_specificiterome54,
D.dc_specificiterome55,
D.dc_specificiterome56,
D.dc_specificiterome57,
D.dc_specificiterome58,
D.dc_specificiterome59,
D.dc_specificiterome60,
D.dc_specificiterome61,
D.dc_specificiterome62,
D.dc_specificiterome63,
D.dc_specificiterome64,
D.dc_specificiterome65,
D.dc_specificiterome66,
D.dc_specificiterome67,
D.dc_specificiterome68,
D.dc_specificiterome69,
D.dc_specificiterome70,
D.dc_specificiterome71,
D.dc_specificiterome72,
D.dc_specificiterome73,
D.dc_specificiterome74,
D.dc_specificiterome75,
D.dc_specificiterome76,
D.dc_specificiterome77,
D.dc_specificiterome78,
D.dc_specificiterome79,
D.dc_specificiterome80
FROM fb00_uda03430.features_tmp1 A
LEFT JOIN (SELECT * , RANK() OVER (
    PARTITION BY kn_individu_national ORDER BY kd_datemodification DESC,
    kn_numpec DESC) AS rk FROM braff00.pr00_ppx005_ddiadh_priseencharge) B
ON A.kn_individu_national = B.kn_individu_national
LEFT JOIN (SELECT * , RANK() OVER (
    PARTITION BY kn_individu_national ORDER BY kd_datemodification DESC)
    AS rk FROM braff00.pr00_ppx005_dcoind_individu) C
ON A.kn_individu_national = C.kn_individu_national
LEFT JOIN (SELECT *, RANK () OVER (
        PARTITION BY kn_individu_national
        ORDER BY kd_datemodification DESC, kc_profilde DESC) AS rk FROM
    braff00.pr00_ppx005_dcopro_profilprofessionnel) D
ON A.kn_individu_national = D.kn_individu_national
WHERE B.rk = 1 AND C.rk = 1 AND D.rk = 1;


beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e "select * from fb00_uda03430.features" > /donapp/uda034/p00/dat/features_2021_D1106.csv
