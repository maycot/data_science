package fr.pe.sire.service.da029

import org.apache.spark.{SparkConf, SparkContext, SQLContext} 

object CalendarTablesSre {

      // parametre 0 : schema Hive
      // parametre 1 : test_start
      // parametre 2 : test_end
      // parametre 3 : days_delta_1
      // parametre 4 : days_delta_2 
      // parametre 5 : days_delta_3 

      def main(args: Array[String]) {

        val spark = SparkSession
          .builder()
          .appName("Generation des tables de durees de pec pour SRE")
          .enableHiveSupport()
          .getOrCreate()

        val schema = args(0)
        val test_start = args(1)
        val test_end = args(2)
        val days_delta_1 = args(3)
        val days_delta_2 = args(4)
        val days_delta_3 = args(5)

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          E.kn_numpec,
          E.dc_typepec_id as type_pec,
          E.dd_dateeffetpec,
          E.dd_dateeffetcessationpec,
          E.kd_datemodification
          FROM """+ schema +""".dataset12_test B
          INNER JOIN """+ schema +""".pr00_ppx005_ddi_priseencharge_tmp E
          ON B.ident = E.kc_individu_local
          AND B.bni = E.kn_individu_national
          WHERE E.dc_typepec_id IS NOT NULL
          AND (E.dd_dateeffetpec <> E.dd_dateeffetcessationpec
              OR E.dd_dateeffetcessationpec IS NULL)
          AND TO_DATE(E.kd_datemodification) < TO_DATE(""""+ test_end +"""")
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp1_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.dd_dateeffetpec,
          B.dd_dateeffetcessationpec,
          B.kd_datemodification
          FROM (SELECT *, RANK() OVER(
                  PARTITION BY ident, type_pec, dd_dateeffetpec
                  ORDER BY kd_datemodification DESC) AS rk FROM
              """+ schema +""".pec_tmp1_test) B
          WHERE B.rk = 1
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp2_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.dd_dateeffetpec,
          B.dd_dateeffetcessationpec,
          B.kd_datemodification
          FROM (SELECT *, RANK() OVER(
                  PARTITION BY ident
                  ORDER BY dd_dateeffetpec DESC, kd_datemodification DESC) AS rk FROM
              """+ schema +""".pec_tmp2_test) B
          WHERE B.rk <> 1
          AND B.dd_dateeffetcessationpec IS NOT NULL
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp3_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.dd_dateeffetpec,
          B.dd_dateeffetcessationpec,
          B.kd_datemodification
          FROM (SELECT *, RANK() OVER(
                  PARTITION BY ident
                  ORDER BY dd_dateeffetpec DESC, kd_datemodification DESC) AS rk FROM
              """+ schema +""".pec_tmp2_test) B
          WHERE B.rk = 1
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp4_test")

        var table_tmp = spark.sql("""
          SELECT * FROM (
            SELECT * FROM """+ schema +""".pec_tmp3_test
            UNION ALL
            SELECT * FROM """+ schema +""".pec_tmp4_test
          ) tmp
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp5_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          ident,
          bni,
          kn_numpec,
          type_pec,
          TO_DATE(dd_dateeffetpec) AS datins,
          NVL(TO_DATE(dd_dateeffetcessationpec), TO_DATE(""""+ test_end +"""")) AS datann
          FROM """+ schema +""".pec_tmp5_test
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp6_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec as cat_reg,
          B.datins,
          B.datann
          FROM (SELECT *, RANK() OVER(
                  PARTITION BY ident
                  ORDER BY kn_numpec DESC, datins DESC) AS rk FROM
              """+ schema +""".pec_tmp6_test) B
          WHERE B.rk = 1
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp7_test")

        // Create a day calendar
        var table_tmp = spark.sql("""
          SELECT
          DATE_SUB(TO_DATE(""""+ test_end +""""), pe.i) as date_jour
          FROM """+ schema +""".dual
          LATERAL VIEW POSEXPLODE(SPLIT(SPACE(730), ' ')) pe as i,x
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_calendar_tmp_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann
          FROM """+ schema +""".pec_tmp6_test B
          WHERE B.type_pec = '11'
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp6_typepec1_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann
          FROM """+ schema +""".pec_tmp6_test B
          WHERE B.type_pec = '12'
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp6_typepec2_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann
          FROM """+ schema +""".pec_tmp6_test B
          WHERE B.type_pec = '13'
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp6_typepec3_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann
          FROM """+ schema +""".pec_tmp6_test B
          WHERE B.type_pec NOT IN ('11', '12', '13')
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp6_typepec4_test")

        // join with calendar

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann,
          C.date_jour
          FROM """+ schema +""".pec_tmp6_typepec4_test B
          JOIN """+ schema +""".pec_calendar_tmp_test C
          WHERE B.datins <= C.date_jour
          AND B.datann >= C.date_jour
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp7_typepec4_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann,
          C.date_jour
          FROM """+ schema +""".pec_tmp6_typepec3_test B
          JOIN """+ schema +""".pec_calendar_tmp_test C
          WHERE B.datins <= C.date_jour
          AND B.datann >= C.date_jour
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp7_typepec3_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann,
          C.date_jour
          FROM """+ schema +""".pec_tmp6_typepec2_test B
          JOIN """+ schema +""".pec_calendar_tmp_test C
          WHERE B.datins <= C.date_jour
          AND B.datann >= C.date_jour
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp7_typepec2_test")

        var table_tmp = spark.sql("""
          SELECT DISTINCT
          B.ident,
          B.bni,
          B.kn_numpec,
          B.type_pec,
          B.datins,
          B.datann,
          C.date_jour
          FROM """+ schema +""".pec_tmp6_typepec1_test B
          JOIN """+ schema +""".pec_calendar_tmp_test C
          WHERE B.datins <= C.date_jour
          AND B.datann >= C.date_jour
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_tmp7_typepec1_test")

        // compute pec features

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec1_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_1 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec1_delta1_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec2_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_1 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec2_delta1_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec3_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_1 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec3_delta1_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec4_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_1 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec4_delta1_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec1_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_2 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec1_delta2_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec2_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_2 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec2_delta2_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec3_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_2 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec3_delta2_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec4_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_2 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec4_delta2_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec1_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_3 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec1_delta3_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec2_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_3 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec2_delta3_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec3_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_3 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec3_delta3_tmp_test")

        var table_tmp = spark.sql("""
          SELECT 
          ident,
          COUNT(DISTINCT kn_numpec) AS pec_count,
          COUNT(DISTINCT date_jour) AS pec_days_count
          FROM """+ schema +""".pec_tmp7_typepec4_test
          WHERE date_jour >=  DATE_SUB(TO_DATE(""""+ test_end +""""), """+ days_delta_3 +""")
          GROUP BY ident
        """)

        table_tmp.write.format("orc").mode("overwrite").saveAsTable(schema+".pec_metrics_typepec4_delta3_tmp_test")

        spark.stop()
      }
}
