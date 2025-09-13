-- Delai pourvoi - Step 4 : Merge mec, dpae et gaec

-- Toutes les Offres avec comme labellisation:
-- 1. si on trouve merplus alors date acceptation mer+
-- 2. sinon si on trouve dpae alors date creation ami
-- 3. sinon si on trouve gaec alors date creation ami
DROP TABLE IF EXISTS fb00_uda03430.base_offre_mecacceptee_dpae_gaec_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.base_offre_mecacceptee_dpae_gaec_global
STORED AS ORC
AS SELECT DISTINCT
A.dn_etablissement,
A.kc_offre,
A.dd_dateanulationoffre,
A.dc_motifmiseajour_1_id,
A.dc_motifmiseajour_2_id,
A.dd_datecreationreport,
CASE WHEN D.kc_offre IS NOT NULL THEN 'mer_plus'
WHEN B.kc_offre IS NOT NULL THEN 'dpae'
WHEN C.kc_offre IS NOT NULL THEN 'gaec' END AS contrat,
B.datecreation_ami as datecreation_ami_dpae,
B.date_creation_dpae,
B.date_embauche_premiere_dpae,
C.datecreation_ami as datecreation_ami_gaec,
C.date_activite_premiere_gaec,
D.date_acceptation_premiere_mec_acceptee
FROM fb00_uda03430.offres_pe_last A
LEFT JOIN fb00_uda03430.premiere_dpae_global B
ON A.kc_offre = B.kc_offre
LEFT JOIN fb00_uda03430.premiere_gaec_global C
ON A.kc_offre = C.kc_offre
LEFT JOIN fb00_uda03430.premiere_mec_acceptee_global D
ON A.kc_offre = D.kc_offre;
