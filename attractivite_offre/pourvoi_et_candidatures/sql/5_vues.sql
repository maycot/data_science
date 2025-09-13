-- set  SCHEMA = fb00_uda03430;
-- set SCHEMA;
-- set  SCHEMA_DATALAKE = braff00;
-- set SCHEMA_DATALAKE;

DROP TABLE IF EXISTS fb00_uda03430.navigationsitepe_sample;
CREATE TABLE fb00_uda03430.navigationsitepe_sample
STORED AS ORC AS SELECT DISTINCT
dc_numoffre,
dc_servicesutilisateur,
dd_dateevenement,
dn_internauteconnecte,
dc_identifianttemporairesession
FROM braff00.pr00_pxitix_navigationsitepe
WHERE dd_dateevenement >= DATE_SUB(CURRENT_DATE, 120)
AND LENGTH(dc_numoffre) > 2
AND dc_arborescencepagen1 in (
--'Detail offre::Depuis liste offres::Detail offre partenaire',
'Detail offre::Depuis detail offre::Detail offre PE',
'Detail_offre',
'Detail offre::Depuis liste offres::Detail offre PE',
'Detail offre::Standalone::Detail offre PE',
--'Detail offre::Standalone::Detail offre partenaire',
'Detail offre::Ma selection offres::Detail offre PE',
'Detail offre::Depuis candidature::Detail offre PE')
AND dc_identifianttemporairesession IS NOT NULL;

-- Offres PE
DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_tmp1;
CREATE TABLE fb00_uda03430.dataset_vues_tmp1
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
B.dd_dateevenement,
SUBSTR(B.dd_dateevenement, 0, 10) as jour,
B.dc_identifianttemporairesession
FROM (SELECT * FROM fb00_uda03430.offres_pe WHERE dd_datecreationreport >= DATE_SUB(CURRENT_DATE, 120)) A
LEFT JOIN fb00_uda03430.navigationsitepe_sample B
ON A.kc_offre = B.dc_numoffre;

-- Suppression des robots
DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_robot_tmp1;
CREATE TABLE fb00_uda03430.dataset_vues_robot_tmp1
STORED AS ORC AS SELECT
dc_identifianttemporairesession,
jour,
COUNT(DISTINCT dd_dateevenement) as action_count
FROM fb00_uda03430.dataset_vues_tmp1
GROUP BY dc_identifianttemporairesession, jour;

DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_robot_tmp2;
CREATE TABLE fb00_uda03430.dataset_vues_robot_tmp2
STORED AS ORC
AS SELECT DISTINCT
dc_identifianttemporairesession
FROM fb00_uda03430.dataset_vues_robot_tmp1
WHERE action_count > 70;

DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_tmp5;
CREATE TABLE fb00_uda03430.dataset_vues_tmp5
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.dd_dateevenement,
A.jour,
A.dc_identifianttemporairesession
FROM fb00_uda03430.dataset_vues_tmp1 A
LEFT JOIN fb00_uda03430.dataset_vues_robot_tmp2 B
ON A.dc_identifianttemporairesession = B.dc_identifianttemporairesession
WHERE B.dc_identifianttemporairesession IS NULL;

DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_tmp6;
CREATE TABLE fb00_uda03430.dataset_vues_tmp6
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
B.dd_dateevenement,
B.jour,
B.dc_identifianttemporairesession
FROM (SELECT * FROM fb00_uda03430.offres_pe WHERE dd_datecreationreport >= DATE_SUB(CURRENT_DATE, 120)) A
LEFT JOIN fb00_uda03430.dataset_vues_tmp5 B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.features_vues_tmp7;
CREATE TABLE fb00_uda03430.features_vues_tmp7
STORED AS ORC AS SELECT
kc_offre,
jour,
COUNT(DISTINCT dc_identifianttemporairesession) as vues_count
FROM fb00_uda03430.dataset_vues_tmp6
GROUP BY kc_offre, jour;

DROP TABLE IF EXISTS fb00_uda03430.features_vues_tmp8;
CREATE TABLE fb00_uda03430.features_vues_tmp8
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
B.jour,
B.vues_count
FROM fb00_uda03430.dataset_vues_tmp6 A 
LEFT JOIN fb00_uda03430.features_vues_tmp7 B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_pe;
CREATE TABLE fb00_uda03430.dataset_vues_pe
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_rome_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_acteur_miseajour1_id,
A.dc_topalertqltoffpremverif,
A.dc_topalertqltoffextraction,
A.dc_topdesquepossible,
A.dn_etablissement,
SUBSTR(A.dc_rome_id, 0, 3) as domaine_pro,
A.naf,
A.dc_appelationrome_id,
A.dc_typesalaire,
A.dc_typexperienceprof_id,
A.dc_typecontrat_id,
A.dc_typeservicerecrutement_1_id,
A.dc_modepreselection_id,
A.dc_typeaffichage_id,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.dc_trancheeffectifetab,
A.dep,
A.dc_communelieutravail,
A.dc_typeformation_1_id,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_unitedureecontrat,
A.dc_categorie_contrat,
A.dc_categorie_experience,
A.dc_categorie_dureetravailhebdoheures,
A.dn_statutetablissement,
A.dc_saisonnalite,
A.dc_idcdifficulteeconomique_id,
SUBSTR(A.naf, 0, 3) as naf3,
A.dc_topinternet,
B.jour,
B.vues_count
FROM (SELECT * FROM fb00_uda03430.offres_pe WHERE dd_datecreationreport >= DATE_SUB(CURRENT_DATE, 120)) A
LEFT JOIN fb00_uda03430.features_vues_tmp8 B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.dataset_vues_pe_counter;
CREATE TABLE fb00_uda03430.dataset_vues_pe_counter
STORED AS ORC AS SELECT
kc_offre,
dd_datecreationreport,
SUM(vues_count) as total_vues
FROM fb00_uda03430.dataset_vues_pe
GROUP BY kc_offre, dd_datecreationreport;

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e "select * from fb00_uda03430.dataset_vues_pe" > /donapp/uda034/p00/dat/lac_dataset_vues_pe.csv

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e "select * from fb00_uda03430.dataset_vues_pe_counter" > /donapp/uda034/p00/dat/vues_counter.csv