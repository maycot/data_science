-- Delai pourvoi - Step 3 : GAEC

-- Toutes les GAEC (de type 11, 7, 9, 4, 15) sur période 
DROP TABLE IF EXISTS fb00_uda03430.base_activite_gaec_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.base_activite_gaec_global
STORED AS ORC
AS SELECT DISTINCT
A.dd_datedebutactivite,
A.kn_individu_national,
A.dn_entreprise_id AS rce
FROM (SELECT *, ROW_NUMBER() OVER (
      PARTITION BY kn_individu_national, kn_periodeactivitegaec 
      ORDER BY dd_datemodification DESC,
      td_datmaj DESC, tn_numevt DESC) AS rk
      FROM braff00.pr00_ppx005_drvder_periodeactivitegaec
      WHERE yearraff >= "2019") A
WHERE A.rk = 1
AND A.dc_typeperiodegaec_id in ("11", "7", "9", "4", "15")
AND A.dn_entreprise_id IS NOT NULL
AND A.dd_datedebutactivite >= '2019-01-01';

-- On récup tous les couples (individu_national, rce)
-- présents dans les 2 tables (ami et gaec)
-- On filtre les gaec dont la date est > 4 mois 
-- (120 jours, borne_sup)
-- Attention les GAEC sont des contrats courts donc
-- potentiellement beaucoup de lignes pour une même offre
DROP TABLE IF EXISTS fb00_uda03430.mec_avec_gaec_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.mec_avec_gaec_global
STORED AS ORC
AS SELECT DISTINCT
A.kc_offre, 
A.rce, 
A.dn_individu_national,
A.datecreation_ami,
B.dd_datedebutactivite as debut_activite_gaec
FROM fb00_uda03430.bornes_mec_global A
INNER JOIN fb00_uda03430.base_activite_gaec_global B
ON (A.rce = B.rce AND A.dn_individu_national = B.kn_individu_national)
WHERE B.dd_datedebutactivite > A.borne_inf
AND B.dd_datedebutactivite <= A.borne_sup;

-- 1ere GAEC enregistrée
-- On ne conserve que la 1ere GAEC enregistrée sur l'offre
DROP TABLE IF EXISTS fb00_uda03430.premiere_gaec_global;
CREATE TABLE IF NOT EXISTS fb00_uda03430.premiere_gaec_global
STORED AS ORC
AS SELECT DISTINCT
kc_offre,
A.datecreation_ami,
debut_activite_gaec AS date_activite_premiere_gaec
FROM (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY kc_offre ORDER BY debut_activite_gaec ASC) AS rk
    FROM fb00_uda03430.mec_avec_gaec_global) A
WHERE A.rk = 1;
