-- Delai pourvoi Step 1 : Selection des mec

-- OFFRES AVEC MEC ACCEPTEE
-- Détecter les offres sujettes à une MEC acceptee : offres avec un code d'etat de 
-- l'AMI en CDT-ACC, MPD-ACC, MPO-ACC et telles que la date de l'AMI soit sup à
-- la creation de l'offre

-- Toutes les ami sur les offres de la base
DROP TABLE IF EXISTS fb00_uda03430.ami_tmp1;
CREATE TABLE IF NOT EXISTS fb00_uda03430.ami_tmp1
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.dn_etablissement AS rce,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
B.kn_ami,
B.dd_datecreation,
B.dn_individu_national
FROM fb00_uda03430.offres_pe_last A
INNER JOIN (SELECT *, RANK () OVER (PARTITION BY kn_ami
    ORDER BY kn_derniersuivi DESC) AS rk FROM braff00.pr00_ppx005_tinmer_ami
    WHERE yearraff >= '2019') B
ON A.kc_offre = B.dc_competencerecherchee
WHERE B.rk = 1
AND TO_DATE(B.dd_datecreation) >= TO_DATE(A.dd_datecreationreport)
AND TO_DATE(B.dd_datecreation) < TO_DATE(A.dd_dateanulationoffre);

-- Toutes les mec acceptées sur les offres de la base
DROP TABLE IF EXISTS fb00_uda03430.mec_acceptees;
CREATE TABLE IF NOT EXISTS fb00_uda03430.mec_acceptees
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.rce,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.kn_ami,
A.dd_datecreation,
B.dd_datecreation as date_acceptation
FROM fb00_uda03430.ami_tmp1 A
INNER JOIN (SELECT * FROM braff00.pr00_ppx005_tinmer_lignesuiviami WHERE yearraff >= "2019") B
ON A.kn_ami = B.dn_ami
WHERE B.dc_etatami_id IN ('CDT-ACC', 'MPD-ACC', 'MPO-ACC');

-- 1ere MEC acceptée
DROP TABLE IF EXISTS fb00_uda03430.premiere_mec_acceptee_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.premiere_mec_acceptee_global
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.rce,
A.dd_datecreation AS date_creation_premiere_mec_acceptee,
A.date_acceptation AS date_acceptation_premiere_mec_acceptee
FROM (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY kc_offre ORDER BY date_acceptation ASC) AS rk
    FROM fb00_uda03430.mec_acceptees) A
WHERE A.rk = 1;

-- Tous les AMIs uniques des offres sauf les REF (profils refusés)
DROP TABLE IF EXISTS fb00_uda03430.ami_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.ami_global
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.rce,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.kn_ami,
A.dd_datecreation,
A.dn_individu_national
FROM fb00_uda03430.ami_tmp1 A
INNER JOIN (SELECT * FROM braff00.pr00_ppx005_tinmer_lignesuiviami WHERE yearraff >= "2019") B
ON A.kn_ami = B.dn_ami
WHERE B.dc_etatami_id NOT IN ('PRF-REF','PCO-REF','PCE-REF', 'MPO-REF','MPD-REF','CDT-REF');

-- Pour chaque AMI on ajoute une période de 4 mois après sa création, on va récupérer toutes les 
-- DPAE ou GAEC créées max 4 mois après la date de création de l'AMI
DROP TABLE IF EXISTS fb00_uda03430.bornes_mec_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.bornes_mec_global
STORED AS ORC
AS SELECT DISTINCT  
A.kc_offre,
A.rce,
A.dd_datecreationreport,
A.dd_dateanulationoffre,
A.kn_ami,
A.dd_datecreation AS datecreation_ami,
A.dn_individu_national,
A.dd_datecreationreport AS borne_inf, 
DATE_ADD(A.dd_datecreation, 270) AS borne_sup
FROM fb00_uda03430.ami_global A;

