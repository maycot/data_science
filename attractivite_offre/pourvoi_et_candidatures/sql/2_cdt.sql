-- set  SCHEMA = fb00_uda03430;
-- set SCHEMA;
-- set  SCHEMA_DATALAKE = braff00;
-- set SCHEMA_DATALAKE;

DROP TABLE IF EXISTS fb00_uda03430.ami_cdt_tmp1;
CREATE TABLE fb00_uda03430.ami_cdt_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.dn_ami,
A.dd_datecreation
FROM braff00.pr00_ppx005_tinmer_lignesuiviami A
WHERE A.dd_datecreation > DATE_SUB(CURRENT_DATE, 15)
--WHERE A.dd_datecreation >= '2022-04-20'
AND (A.dc_etatami_id = 'CDT-CREE' AND A.dc_acteelementaireami_id = 'CDT-CRE')
OR A.dc_etatami_id = 'CAT-CREE';

DROP TABLE IF EXISTS fb00_uda03430.ami_cdt_tmp2;
CREATE TABLE fb00_uda03430.ami_cdt_tmp2
STORED AS ORC AS SELECT DISTINCT
A.dn_ami,
A.dd_datecreation,
B.dc_competencerecherchee AS kc_offre
FROM fb00_uda03430.ami_cdt_tmp1 A
INNER JOIN (SELECT *, RANK () OVER (PARTITION BY kn_ami ORDER BY kn_derniersuivi DESC, dd_datemodification DESC, tn_numevt DESC, date_insertion_clean DESC) AS rk FROM braff00.pr00_ppx005_tinmer_ami) B
ON A.dn_ami = B.kn_ami
WHERE B.rk = 1
AND B.dc_competencerecherchee IS NOT NULL;

DROP TABLE IF EXISTS fb00_uda03430.ami_cdt_tmp3;
CREATE TABLE fb00_uda03430.ami_cdt_tmp3
STORED AS ORC AS SELECT DISTINCT
A.dn_ami,
SUBSTR(A.dd_datecreation, 0, 10) as cdt_jour,
B.kc_offre,
B.dd_datecreationreport
FROM fb00_uda03430.ami_cdt_tmp2 A
INNER JOIN fb00_uda03430.offres_pe B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.ami_cdt_tmp4;
CREATE TABLE fb00_uda03430.ami_cdt_tmp4
STORED AS ORC AS SELECT
kc_offre,
cdt_jour,
COUNT(DISTINCT dn_ami) AS cdt_count
FROM fb00_uda03430.ami_cdt_tmp3
GROUP BY kc_offre, cdt_jour;

DROP TABLE IF EXISTS fb00_uda03430.ami_cdt_tmp5;
CREATE TABLE fb00_uda03430.ami_cdt_tmp5
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
B.cdt_jour,
B.cdt_count
FROM fb00_uda03430.ami_cdt_tmp3 A
INNER JOIN fb00_uda03430.ami_cdt_tmp4 B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.dataset_cdt_pe;
CREATE TABLE fb00_uda03430.dataset_cdt_pe
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_rome_id,
A.dc_diffusionserveur_id,
A.dn_nbrpostesoffertscreation,
A.dd_datemaj1,
A.dc_qualification_id,
A.dc_motifmiseajour_1_id,
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
B.cdt_jour,
B.cdt_count
FROM fb00_uda03430.offres_pe A
LEFT JOIN fb00_uda03430.ami_cdt_tmp5 B
ON A.kc_offre = B.kc_offre;

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e 'select * from fb00_uda03430.dataset_cdt_pe' > /donapp/uda034/p00/dat/lac_cdt_predict1.csv

beeline -u 'jdbc:hive2://sbfn01.sip24.pole-emploi.intra:2181,sbfn02.sip24.pole-emploi.intra:2181,sbfe02.sip24.pole-emploi.intra:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;hive.support.concurrency=true;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;hive.compactor.initiator.on=true;hive.compactorworker.threads=1;' --outputformat=dsv -e "select * from fb00_uda03430.dataset_cdt_pe" > /donapp/uda034/p00/dat/lac_cdt.csv

DROP TABLE IF EXISTS fb00_uda03430.cdt_text_tmp1;
CREATE TABLE fb00_uda03430.cdt_text_tmp1
STORED AS ORC AS SELECT DISTINCT
A.dn_ami,
B.kc_offre
FROM fb00_uda03430.ami_cdt_tmp2 A
INNER JOIN fb00_uda03430.offres_pe B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.cdt_text_tmp2;
CREATE TABLE fb00_uda03430.cdt_text_tmp2
STORED AS ORC AS SELECT
kc_offre,
COUNT(DISTINCT dn_ami) AS cdt_total
FROM fb00_uda03430.cdt_text_tmp1
GROUP BY kc_offre;


DROP TABLE IF EXISTS fb00_uda03430.cdt_text_tmp3;
CREATE TABLE fb00_uda03430.cdt_text_tmp3
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.dd_datecreationreport,
A.dc_modepreselection_id,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_typeaffichage_id,
A.dep,
A.dc_communelieutravail,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_rome_id,
A.naf,
A.dc_trancheeffectifetab,
A.dc_appelationrome_id,
A.dn_etablissement,
B.cdt_total
FROM fb00_uda03430.offres_pe A
LEFT JOIN fb00_uda03430.cdt_text_tmp2 B
ON A.kc_offre = B.kc_offre;

DROP TABLE IF EXISTS fb00_uda03430.cdt_text;
CREATE TABLE fb00_uda03430.cdt_text
STORED AS ORC AS SELECT DISTINCT
A.kc_offre,
A.cdt_total,
A.dd_datecreationreport,
A.dc_modepreselection_id,
A.dc_modepresentation_emp_id,
A.dc_modepresentation_agence_id,
A.dc_typeaffichage_id,
A.dep,
A.dc_communelieutravail,
A.dc_intituleoffre,
A.dc_descriptifoffre,
A.dc_descriptifentreprise,
A.dc_rome_id,
A.naf,
A.dc_trancheeffectifetab,
A.dc_appelationrome_id,
A.dn_etablissement,
B.dc_lblappellationromev3,
C.dc_lblrome
FROM fb00_uda03430.cdt_text_tmp3 A
LEFT JOIN braff00.pr00_pdn014_ref_appellationrome B
ON SUBSTRING(A.dc_appelationrome_id, 2) = B.kc_appellationromev3
LEFT JOIN braff00.pr00_pdn014_ref_rome C
ON A.dc_rome_id = C.kc_rome
;

DROP TABLE IF EXISTS fb00_uda03430.dataset_cdt_pe_counter;
CREATE TABLE fb00_uda03430.dataset_cdt_pe_counter
STORED AS ORC AS SELECT
kc_offre,
dd_datecreationreport,
SUM(cdt_count) as total_cdt
FROM fb00_uda03430.dataset_cdt_pe
GROUP BY kc_offre, dd_datecreationreport;

beeline -u 'jdbc:hive2://hp1edge02.pole-emploi.intra:2181,hp1namenode01.pole-emploi.intra:2181,hp1namenode02.pole-emploi.intra:2181/database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;' --outputformat=dsv -e 'select * from fb00_uda03430.dataset_cdt_pe_counter' > /donapp/uda034/p00/dat/cdt_counter.csv
