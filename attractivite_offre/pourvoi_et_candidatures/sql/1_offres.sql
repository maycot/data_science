
-- SCHEMA fb00_uda03430;
-- SCHEMA_DATALAKE = braff00;

-- DATE_CREATION_MIN = DATE_SUB(CURRENT_DATE, 105);

DROP TABLE IF EXISTS fb00_uda03430.offres_pe_tmp1;
CREATE TABLE fb00_uda03430.offres_pe_tmp1
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_motifmiseajour_1_id,
A.dc_motifmiseajour_2_id,
A.kd_datemodification,
A.tn_numevt,
A.dc_naturecontrat_id,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_acteur_miseajour1_id,
A.dc_topalertqltoffpremverif,
A.dc_topalertqltoffextraction,
A.dc_topdesquepossible,
SUBSTR(A.dc_codepostallieutravail, 1, 2) as dep,
A.dc_rome_id,
A.dc_naf2 as naf,
A.dc_appelationrome_id,
A.dc_typesalaire,
A.dc_typexperienceprof_id,
A.dc_typehoraire,
A.dc_typecontrat_id,
A.dc_typeservicerecrutement_1_id,
A.dc_modepreselection_id,
A.dc_typeaffichage_id,
A.dc_topinternet,
A.dd_datecreationreport,
A.dn_etablissement,
A.dd_dateanulationoffre,
A.dc_trancheeffectifetab,
A.dc_communelieutravail,
A.dc_typeformation_1_id,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_duree_contrat_id,
CASE WHEN LOWER(A.dc_unitedureecontrat) = 'null' THEN NULL
ELSE A.dc_unitedureecontrat
END AS dc_unitedureecontrat,
CASE WHEN (A.dc_typecontrat_id = 'CDI' OR A.dc_typecontrat_id = 'DIN') THEN 'CDI'
WHEN CAST(A.dc_duree_contrat_id AS INT) * IF(A.dc_unitedureecontrat = 'MO', 30, 1) > 180 THEN '>6'
WHEN CAST(A.dc_duree_contrat_id AS INT) * IF(A.dc_unitedureecontrat = 'MO', 30, 1) >= 90 THEN '3-6'
WHEN CAST(A.dc_duree_contrat_id AS INT) * IF(A.dc_unitedureecontrat = 'MO', 30, 1) >= 30 THEN '1-3'
WHEN CAST(A.dc_duree_contrat_id AS INT) * IF(A.dc_unitedureecontrat = 'MO', 30, 1) >= 0 THEN '<1'
ELSE 'autre' END AS dc_categorie_contrat,
CASE WHEN A.dn_dureeminexperienceprof = 0 THEN 'debutant'
WHEN A.dn_dureeminexperienceprof < 12 AND A.dc_typdureeexperienceprof = 'MO' THEN '<1'
WHEN (A.dn_dureeminexperienceprof <= 3 AND A.dc_typdureeexperienceprof = 'AN')
OR (A.dn_dureeminexperienceprof <= 36 AND A.dc_typdureeexperienceprof = 'MO') THEN '1-3'
WHEN (A.dn_dureeminexperienceprof > 3 AND A.dc_typdureeexperienceprof = 'AN')
OR (A.dn_dureeminexperienceprof > 36 AND A.dc_typdureeexperienceprof = 'MO') THEN '>3'
ELSE 'autre' END AS dc_categorie_experience,
CASE WHEN A.dn_dureetravailhebdoheures >= 35 THEN '>35'
WHEN A.dn_dureetravailhebdoheures >= 24 THEN '24-35'
WHEN A.dn_dureetravailhebdoheures >= 1 THEN '<24'
ELSE 'autre'END AS dc_categorie_dureetravailhebdoheures
FROM braff00.pr00_ppx005_eofcol_offre A
-- WHERE (A.dd_datecreationreport >= '2022-04-20'
-- AND A.dd_datecreationreport < '2022-05-15')
WHERE A.dd_datecreationreport > DATE_SUB(CURRENT_DATE, 15)
AND A.dc_codepostallieutravail <> 'null'
-- AND A.dc_motifmiseajour_1_id = 'null'
-- AND A.dc_motifmiseajour_2_id = 'null'
-- AND A.dc_motifmiseajour_3_id = 'null'
-- AND A.dc_motifmiseajour_4_id = 'null'
AND A.dc_typeservicerecrutement_1_id in ('APP', 'ACC')
AND A.dc_diffusionserveur_id <> 'N'
AND A.dc_etatoffre_id = 'EC'
AND A.dc_statutoffre_id IN ('G', 'S')
AND A.dc_typologieoffre_id = 'C'
AND A.dc_typemiseajour_1_id NOT IN ('REF', 'SUS', 'ANN', 'DAE')
-- AND A.dc_topalertqltoffpremverif IN ('N', 'S')
-- AND A.dc_topalertqltoffextraction IN ('N', 'S')
AND A.dc_naturecontrat_id NOT IN ('PS', 'NS');

select kd_datemodification, dd_datecreationreport, dc_codepostallieutravail, dc_motifmiseajour_1_id, dc_motifmiseajour_2_id, 
dc_motifmiseajour_3_id, dc_motifmiseajour_4_id, dc_typeservicerecrutement_1_id, dc_diffusionserveur_id,
dc_etatoffre_id, dc_statutoffre_id, dc_typologieoffre_id, dc_typemiseajour_1_id,
dc_topalertqltoffpremverif, dc_topalertqltoffextraction, dc_naturecontrat_id from braff00.pr00_ppx005_eofcol_offre  where kc_offre='124VHNF' ORDER BY kd_datemodification ASC;

DROP TABLE IF EXISTS fb00_uda03430.offres_pe_tmp2;
CREATE TABLE fb00_uda03430.offres_pe_tmp2
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_motifmiseajour_1_id,
A.dc_motifmiseajour_2_id,
A.dc_naturecontrat_id,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_acteur_miseajour1_id,
A.dc_topalertqltoffpremverif,
A.dc_topalertqltoffextraction,
A.dc_topdesquepossible,
A.dep,
A.dc_rome_id,
A.naf,
A.dc_appelationrome_id,
A.dc_typesalaire,
A.dc_typexperienceprof_id,
A.dc_typehoraire,
A.dc_typecontrat_id,
A.dc_typeservicerecrutement_1_id,
A.dc_modepreselection_id,
A.dc_typeaffichage_id,
A.dc_topinternet,
A.dd_datecreationreport,
A.dn_etablissement,
A.dd_dateanulationoffre,
A.dc_trancheeffectifetab,
A.dc_communelieutravail,
A.dc_typeformation_1_id,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_duree_contrat_id,
A.dc_unitedureecontrat,
A.dc_categorie_contrat,
A.dc_categorie_experience,
A.dc_categorie_dureetravailhebdoheures
FROM (SELECT *, RANK () OVER (
    PARTITION BY kc_offre ORDER BY kd_datemodification ASC, tn_numevt DESC
    ) AS rk FROM fb00_uda03430.offres_pe_tmp1) A
WHERE A.rk = 1;


DROP TABLE IF EXISTS fb00_uda03430.offres_pe;
CREATE TABLE fb00_uda03430.offres_pe
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_acteur_miseajour1_id,
A.dc_motifmiseajour_1_id,
A.dc_topalertqltoffpremverif,
A.dc_topalertqltoffextraction,
A.dc_topdesquepossible,
A.dn_etablissement,
A.dc_rome_id,
A.naf,
A.dc_appelationrome_id,
A.dc_typesalaire,
A.dc_typexperienceprof_id,
A.dc_typehoraire,
A.dc_typecontrat_id,
A.dc_typeservicerecrutement_1_id,
A.dc_modepreselection_id,
A.dc_typeaffichage_id,
A.dd_datecreationreport,
A.dc_trancheeffectifetab,
A.dep,
A.dc_communelieutravail,
A.dc_typeformation_1_id,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_duree_contrat_id,
A.dc_unitedureecontrat,
A.dc_categorie_contrat,
A.dc_categorie_experience,
A.dc_categorie_dureetravailhebdoheures,
A.dc_topinternet,
CAST(B.dn_statutetablissement as string) as dn_statutetablissement,
B.dc_saisonnalite,
B.dc_idcdifficulteeconomique_id
FROM fb00_uda03430.offres_pe_tmp2 A
LEFT JOIN (SELECT *, RANK () OVER (PARTITION BY kn_etablissement ORDER BY kd_datemodification DESC, tn_numevt DESC) AS rk FROM braff00.pr00_ppx005_egcemp_etablissement) B
ON A.dn_etablissement = B.kn_etablissement
WHERE B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.offres_pe_last;
CREATE TABLE fb00_uda03430.offres_pe_last
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
A.dn_etablissement,
B.dc_motifmiseajour_1_id,
B.dc_motifmiseajour_2_id,
B.dd_dateanulationoffre
FROM fb00_uda03430.offres_pe A
INNER JOIN (SELECT *, RANK () OVER (
    PARTITION BY kc_offre ORDER BY kd_datemodification DESC, tn_numevt DESC)
     AS rk FROM braff00.pr00_ppx005_eofcol_offre) B
ON A.kc_offre = B.kc_offre
WHERE B.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.offres_pe_predict;
CREATE TABLE fb00_uda03430.offres_pe_predict
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_acteur_miseajour1_id,
A.dc_topalertqltoffpremverif,
A.dc_topalertqltoffextraction,
A.dc_topdesquepossible,
A.dn_etablissement,
A.dc_rome_id,
A.naf,
A.dc_appelationrome_id,
A.dc_typesalaire,
A.dc_typexperienceprof_id,
A.dc_typehoraire,
A.dc_typecontrat_id,
A.dc_typeservicerecrutement_1_id,
A.dc_modepreselection_id,
A.dc_typeaffichage_id,
A.dd_datecreationreport,
A.dc_trancheeffectifetab,
A.dep,
A.dc_communelieutravail,
A.dc_typeformation_1_id,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_duree_contrat_id,
A.dc_unitedureecontrat,
A.dc_categorie_contrat,
A.dc_categorie_experience,
A.dc_categorie_dureetravailhebdoheures,
A.dc_topinternet,
A.dn_statutetablissement,
A.dc_saisonnalite,
A.dc_idcdifficulteeconomique_id
FROM fb00_uda03430.offres_pe A
INNER JOIN fb00_uda03430.offres_pe_last B
ON A.kc_offre = B.kc_offre
WHERE B.dd_dateanulationoffre IS NULL;

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e "select * from fb00_uda03430.offres_pe_predict" > /donapp/uda034/p00/dat/lac_pourvoi_predict.csv
