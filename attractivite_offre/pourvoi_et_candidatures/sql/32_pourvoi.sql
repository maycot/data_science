-- Delai pourvoi - Step 2 : Jointure DPAE

-- Toutes DPAEs sans certaines catégories NAF => car retrouvées dans gaec
-- 7810Z: Activités des agences de placement de main d'oeuvre
-- 7820Z: Activités des agences de travail temporaire
-- 7830Z: Autre mise à disposition de ressources humaines

-- On garde les embauches uniques au delà de date_creation_min
-- (tuples rce, individu_national uniques)
DROP TABLE IF EXISTS fb00_uda03430.base_dpae_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.base_dpae_global
STORED AS ORC
AS SELECT DISTINCT
A.kd_dateembauche,
A.kd_datecreation,
A.dc_individu_national,
A.kc_nir,
A.dn_employeur AS rce,
A.dc_typecontrat_id
FROM (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY dn_employeur, kd_dateembauche, dc_individu_national ORDER BY kd_datecreation DESC,
    td_datmaj DESC, tn_numevt DESC) AS rk
    FROM  braff00.pr00_ppx005_xdpdpa_dpae
    WHERE yearraff >= "2019") A
WHERE A.rk = 1
AND A.kd_dateembauche >= '2019-01-01';


-- On récup tous les couples (individu_national, rce)
-- présents dans les 2 tables (ami et dpae)
-- On filtre les dpae dont la date est > 4 mois 
-- (120 jours, borne_sup)
DROP TABLE IF EXISTS fb00_uda03430.mec_avec_dpae_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.mec_avec_dpae_global
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre, 
A.rce,
A.dn_individu_national,
A.datecreation_ami,
B.kd_datecreation,
B.kd_dateembauche as date_embauche_dpae
FROM fb00_uda03430.bornes_mec_global A
INNER JOIN fb00_uda03430.base_dpae_global B
ON (A.dn_individu_national = B.dc_individu_national
AND A.rce = B.rce)
WHERE B.kd_dateembauche > A.borne_inf
AND B.kd_dateembauche <= A.borne_sup;

-- 1ere DPAE enregistrée sur l'offre avec AMI
DROP TABLE IF EXISTS fb00_uda03430.premiere_dpae_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.premiere_dpae_global
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre,
A.datecreation_ami,
A.kd_datecreation as date_creation_dpae,
A.date_embauche_dpae AS date_embauche_premiere_dpae
FROM (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY kc_offre ORDER BY date_embauche_dpae ASC) AS rk
    FROM fb00_uda03430.mec_avec_dpae_global) A
WHERE A.rk = 1;


