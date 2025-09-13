-- Delai pourvoi - Step 6 

-- 1. On caractérise le pourvoi pour les offres avec 
-- contrat trouvé (mer_plus ou dpae/gaec) => min(date annulation, date activité)
-- et pour les abandonnés (-1)
DROP TABLE IF EXISTS fb00_uda03430.pourvoi_global_contrat;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pourvoi_global_contrat
STORED AS ORC
AS SELECT DISTINCT
A.dn_etablissement,
A.kc_offre,
A.dd_dateanulationoffre,
A.dc_motifmiseajour_1_id,
A.dd_datecreationreport,
A.contrat,
A.date_embauche_premiere_dpae,
A.datecreation_ami_dpae,
A.date_creation_dpae,
A.date_activite_premiere_gaec,
A.datecreation_ami_gaec,
A.date_acceptation_premiere_mec_acceptee,
-- On prend le min entre la date de création de l'ami et la date d'embauche 
-- car certaines offres sont clôturées avec du retard.
CASE WHEN contrat = "mer_plus" THEN LEAST(
    DATEDIFF(date_acceptation_premiere_mec_acceptee, dd_datecreationreport),
    DATEDIFF(date_embauche_premiere_dpae, dd_datecreationreport),
    DATEDIFF(dd_dateanulationoffre, dd_datecreationreport)
)
WHEN contrat = "dpae" THEN LEAST(
    DATEDIFF(date_embauche_premiere_dpae, dd_datecreationreport),
    DATEDIFF(dd_dateanulationoffre, dd_datecreationreport)
)
WHEN contrat = "gaec" THEN LEAST(
    DATEDIFF(date_activite_premiere_gaec, dd_datecreationreport),
    DATEDIFF(date_creation_dpae, dd_datecreationreport),
    DATEDIFF(dd_dateanulationoffre, dd_datecreationreport)
)
WHEN ((contrat IS NULL) AND (dc_motifmiseajour_1_id IN ("ABD", "RAD", "DIS"))) THEN -1
ELSE NULL
END AS delai_vie
FROM fb00_uda03430.base_offre_mecacceptee_dpae_gaec_global A;

-- On caractérise le pourvoi pour les offres avec un motif
-- de clôture connu ou CLA mais pas de contrat comme le premier
-- poste déclaré pourvu s'il existe
DROP TABLE IF EXISTS fb00_uda03430.pourvoi_global_sanscontrat_premier_postedeclare;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pourvoi_global_sanscontrat_premier_postedeclare
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre, 
A.kd_datemodification,
DATEDIFF(A.dd_dateanulationoffre, B.dd_datecreationreport) AS delai_vie,
B.dd_datecreationreport,
B.dn_etablissement,
B.dd_dateanulationoffre,
B.dc_motifmiseajour_1_id,
B.contrat,
B.date_embauche_premiere_dpae,
B.datecreation_ami_dpae,
B.date_creation_dpae,
B.date_activite_premiere_gaec,
B.datecreation_ami_gaec,
B.date_acceptation_premiere_mec_acceptee
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY kc_offre
        ORDER BY kd_datemodification DESC, tn_numevt DESC) as rk
    FROM braff00.pr00_ppx005_eofcol_offre
    WHERE (
        dn_cmlpostessatisfaitshorsmer > 0 OR
        dn_cmlpostessatisfaitsmerplus > 0 OR
        dn_cmlpostespourvusinterne > 0)
        ) A
INNER JOIN (
    SELECT *
    FROM fb00_uda03430.pourvoi_global_contrat
    WHERE ((contrat IS NULL) 
            AND (dc_motifmiseajour_1_id IN ('CPE','SAN','PEI','SPM','SHM','SPE','CLA'))
    )
) B
ON A.kc_offre = B.kc_offre
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.pourvoi_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pourvoi_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.dn_etablissement,
A.kc_offre,
CASE WHEN
A.delai_vie IS NULL AND B.delai_vie IS NOT NULL THEN B.delai_vie
WHEN 
A.delai_vie IS NOT NULL AND B.delai_vie IS NULL THEN A.delai_vie
ELSE NULL 
END AS delai_vie,
A.delai_vie as delai_vie_contrat,
B.delai_vie as delai_vie_sanscontrat,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.date_embauche_premiere_dpae,
A.datecreation_ami_dpae,
A.date_creation_dpae,
A.date_activite_premiere_gaec,
A.datecreation_ami_gaec,
A.date_acceptation_premiere_mec_acceptee,
A.dc_motifmiseajour_1_id,
A.contrat
FROM fb00_uda03430.pourvoi_global_contrat A
LEFT JOIN fb00_uda03430.pourvoi_global_sanscontrat_premier_postedeclare B
ON A.kc_offre = B.kc_offre;

-- certaines offres ont plusieurs délais de vie, on garde le 1er
DROP TABLE IF EXISTS fb00_uda03430.pourvoi_tmp2;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pourvoi_tmp2
STORED AS ORC
AS SELECT *
FROM (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY kc_offre ORDER BY delai_vie ASC) AS rk
    FROM fb00_uda03430.pourvoi_tmp1) A
WHERE A.rk = 1;

DROP TABLE IF EXISTS fb00_uda03430.pourvoi;
CREATE TABLE IF NOT EXISTS fb00_uda03430.pourvoi
STORED AS ORC
AS SELECT DISTINCT
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
A.dc_idcdifficulteeconomique_id,
B.delai_vie,
B.delai_vie_contrat,
B.delai_vie_sanscontrat,
B.contrat,
B.dd_dateanulationoffre,
B.dc_motifmiseajour_1_id
FROM fb00_uda03430.offres_pe A
LEFT JOIN fb00_uda03430.pourvoi_tmp2 B
ON A.kc_offre = B.kc_offre;

beeline -u 'jdbc:hive2://sbfn01.sip24.pole-emploi.intra:2181,sbfn02.sip24.pole-emploi.intra:2181,sbfe02.sip24.pole-emploi.intra:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;hive.support.concurrency=true;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;hive.compactor.initiator.on=true;hive.compactorworker.threads=1;' --outputformat=dsv -e "select * from fb00_uda03430.pourvoi" > /donapp/uda034/p00/dat/lac_pourvoi.csv
