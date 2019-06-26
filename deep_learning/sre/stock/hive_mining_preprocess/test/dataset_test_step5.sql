/************* SRE-V2 DATASET TEST STEP 5 
***************/

-- Concat all typepec and days delta metrics

DROP TABLE IF EXISTS ${hiveconf:SCHEMA}.pec_metrics_test;
CREATE TABLE IF NOT EXISTS ${hiveconf:SCHEMA}.pec_metrics_test
STORED AS ORC
AS SELECT DISTINCT
A.ident,
A.bni,
NVL(B.pec_count, 0) AS type1_delta1_pec_count,
NVL(B.pec_days_count, 0) AS type1_delta1_pec_days_count,
NVL(C.pec_count, 0) AS type2_delta1_pec_count,
NVL(C.pec_days_count, 0) AS type2_delta1_pec_days_count,
NVL(D.pec_count, 0) AS type3_delta1_pec_count,
NVL(D.pec_days_count, 0) AS type3_delta1_pec_days_count,
NVL(E.pec_count, 0) AS type4_delta1_pec_count,
NVL(E.pec_days_count, 0) AS type4_delta1_pec_days_count,
NVL(I.pec_count, 0) AS type1_delta2_pec_count,
NVL(I.pec_days_count, 0) AS type1_delta2_pec_days_count,
NVL(J.pec_count, 0) AS type2_delta2_pec_count,
NVL(J.pec_days_count, 0) AS type2_delta2_pec_days_count,
NVL(K.pec_count, 0) AS type3_delta2_pec_count,
NVL(K.pec_days_count, 0) AS type3_delta2_pec_days_count,
NVL(L.pec_count, 0) AS type4_delta2_pec_count,
NVL(L.pec_days_count, 0) AS type4_delta2_pec_days_count,
NVL(P.pec_count, 0) AS type1_delta3_pec_count,
NVL(P.pec_days_count, 0) AS type1_delta3_pec_days_count,
NVL(Q.pec_count, 0) AS type2_delta3_pec_count,
NVL(Q.pec_days_count, 0) AS type2_delta3_pec_days_count,
NVL(R.pec_count, 0) AS type3_delta3_pec_count,
NVL(R.pec_days_count, 0) AS type3_delta3_pec_days_count,
NVL(S.pec_count, 0) AS type4_delta3_pec_count,
NVL(S.pec_days_count, 0) AS type4_delta3_pec_days_count
FROM ${hiveconf:SCHEMA}.pec_tmp6_test A
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta1_tmp_test B
ON A.ident = B.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta1_tmp_test C
ON A.ident = C.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta1_tmp_test D
ON A.ident = D.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta1_tmp_test E
ON A.ident = E.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta2_tmp_test I
ON A.ident = I.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta2_tmp_test J
ON A.ident = J.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta2_tmp_test K
ON A.ident = K.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta2_tmp_test L
ON A.ident = L.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta3_tmp_test P
ON A.ident = P.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta3_tmp_test Q
ON A.ident = Q.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta3_tmp_test R
ON A.ident = R.ident
LEFT JOIN ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta3_tmp_test S
ON A.ident = S.ident
;

/*** DATA SUPERVISION ***/

use fb00_uda03423;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'pec_tmp7_test', count(*)
FROM pec_tmp7_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'pec_tmp7_typepec1_test', count(*)
FROM pec_tmp7_typepec1_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'pec_metrics_typepec1_delta1_tmp_test', count(*)
FROM pec_metrics_typepec1_delta1_tmp_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'pec_metrics_typepec1_delta2_tmp_test', count(*)
FROM pec_metrics_typepec1_delta2_tmp_test;

INSERT INTO TABLE sre_test_supervision (
    year, month_start, month_end, table_name, ident_count)
SELECT year(TO_DATE(${hiveconf:test_end})), month(TO_DATE(${hiveconf:test_start})),
    month(DATE_SUB(TO_DATE(${hiveconf:test_end}), 1)), 'pec_metrics_typepec1_delta3_tmp_test', count(*)
FROM pec_metrics_typepec1_delta3_tmp_test;

DROP TABLE ${hiveconf:SCHEMA}.pr00_ppx005_ddi_priseencharge_tmp; 
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp1_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp2_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp3_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp4_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp5_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp6_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp6_typepec1_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp6_typepec2_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp6_typepec3_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp6_typepec4_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec4_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec3_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec2_tmp1_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec2_tmp2_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec2_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec1_tmp1_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec1_tmp2_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec1_tmp3_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec1_tmp4_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_tmp7_typepec1_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta1_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta1_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta1_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta1_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta2_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta2_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta2_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta2_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec1_delta3_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec2_delta3_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec3_delta3_tmp_test;
DROP TABLE ${hiveconf:SCHEMA}.pec_metrics_typepec4_delta3_tmp_test;
