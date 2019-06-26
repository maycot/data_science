# -*- coding: utf-8 -*-

""" Preprocessing data (step 1) : Apply David and other rules to dataset
    Encode data to feed Random Forest model and / or MLP
"""

import sys
sys.path.append('..')
import pandas as pd
import numpy as np
import os
from time import time
from collections import Counter

from sklearn.base import BaseEstimator, TransformerMixin

col_to_drop = ["datnais", "mobunit", "mobdist", "rsa", "nivfor",
               "obligemc", "qualif", "exper", "temps", "nb_enf", "salunit",
               "salmt", "qpv", "diplome", "sitmat", "axe_trav", "conssms",
               "consmail", "entrep", "ctp"]

class Preprocess(BaseEstimator, TransformerMixin):

    def __init__(self):
        pass

    def fit(self, df, y=None):
        return self

    def transform(self, df, y=None):
        start = time()
        df = df[(df.rome.notnull()) & (df.bassin.notnull()) &
                (df.motins.notnull()) & (df.ale.notnull()) &
                (df.cat_reg.notnull())]
        df.ict1 = df.ict1.astype(int)

        # convert to datetime
        date_col = ["datnais", "datins"]
        for col in date_col:
            df[col] = pd.to_datetime(df[col])

        # age at registration date
        registration_year = df.datins.apply(lambda x: x.year)
        datenais_year = df.datnais.apply(lambda x: x.year)
        df["age"] = registration_year - datenais_year

        # age_categ
        bins = [0, 23, 33, 50, 99]
        group_names = ['0', '1', '2', '3']
        df["age_categ"] = pd.cut(df.age, bins = bins, labels=group_names)

        # month year of datins
        df['month'] = df.datins.apply(lambda x: x.month)
        df['year'] = df.datins.apply(lambda x: x.year)

        # trim
        df["trim"] = 1
        df.at[(df["month"] <= 6) & (df["month"] > 3), "trim"] = 2
        df.at[(df["month"] <= 9) & (df["month"] > 6), "trim"] = 3
        df.at[(df["month"] <= 12) & (df["month"] > 9), "trim"] = 4

        # rome 1
        df["r1"] = df.rome.apply(lambda x: x[:1])

        # domaine_pro
        df["domaine_pro"] = df.rome.apply(lambda x: x[:3])
        # drop taxidermie domaine pro
        df = df[df.domaine_pro != "B17"]

        #numpec
        df.numpec = df.numpec.astype(int) / 10
        df["npec"] = 0
        df.at[(df["numpec"] > 1) & (df["numpec"] < 3), "npec"] = 1
        df.at[(df["numpec"] >= 3) & (df["numpec"] <= 6), "npec"] = 2
        df.at[df["numpec"] > 6, "npec"] = 3

        # mobility cat
        df["mobil"] = 0
        df.mobdist = df.mobdist.astype('int')
        df.at[(df["mobunit"] == "MN") & (df["mobdist"] >= 60), "mobil"] = 2
        df.at[(df["mobunit"] == "H") & (df["mobdist"] >= 1), "mobil"] = 2
        df.at[(df["mobunit"] == "KM") & (df["mobdist"] > 39), "mobil"] = 2

        df.at[(df["mobunit"] == "MN") & (df["mobdist"] >= 30) &
                (df["mobil"] != 2), "mobil"] = 1
        df.at[(df["mobunit"] == "H") & (df["mobdist"] < 1) &
                (df["mobdist"] > 0) & (df["mobil"] != 2), "mobil"] = 1
        df.at[(df["mobunit"] == "KN") & (df["mobdist"] > 14) &
                (df["mobil"] != 2), "mobil"] = 1

        # Benefrsa
        df["benefrsa"] = 0
        df.at[df.rsa.notnull(), "benefrsa"] = 1

        # form
        df["form"] = 0
        df.at[df["nivfor"] == "AFS", "form"] = 1
        df.at[df.nivfor.isin(("C12", "C3A","CFG","CP4")), "form"] = 2
        df.at[df["nivfor"] == "NV5", "form"] = 3
        df.at[df["nivfor"] == "NV4", "form"] = 4
        df.at[df["nivfor"] == "NV3", "form"] = 5
        df.at[df["nivfor"] == "NV2", "form"] = 6
        df.at[df["nivfor"] == "NV1", "form"] = 7

        # th
        df["th"] = 0
        df.at[df.obligemc.notnull(), "th"] = 1

        # qual
        df["qual"] = 0
        df.at[df["qualif"] == '1', "qual"] = 1
        df.at[df["qualif"] == '2', "qual"] = 2
        df.at[df["qualif"] == '3', "qual"] = 3
        df.at[df["qualif"] == '4', "qual"] = 4
        df.at[df["qualif"] == '5', "qual"] = 5
        df.at[df["qualif"] == '6', "qual"] = 6
        df.at[df["qualif"] == '7', "qual"] = 7
        df.at[df["qualif"] == '8', "qual"] = 8
        df.at[df["qualif"] == '9', "qual"] = 9

        # motif
        df["motif"] = 0
        df.at[df.motins.isin(('22', '29', '30', '35')), "motif"] = 1
        df.at[df.motins.isin(('21', '24')), "motif"] = 2
        df.at[df['motins'] == '34', "motif"] = 3
        df.at[df['motins'] == '12', "motif"] = 4
        df.at[df['motins'] == '14', "motif"] = 5
        df.at[df['motins'] == '15', "motif"] = 6
        df.at[df['motins'] == '32', "motif"] = 7
        df.at[df['motins'] == '13', "motif"] = 8

        # exper_classe
        df["exper_classe"] = 4
        df.exper = df.exper.astype('int')
        df.at[df["exper"] <= 2, "exper_classe"] = 1
        df.at[(df["exper"] > 2) & (df["exper"] <= 5), "exper_classe"] = 2
        df.at[(df["exper"] > 5) & (df["exper"] <= 10), "exper_classe"] = 3

        # temps plein
        df["temps_plein"] = 0
        df.at[df["temps"] == "1", "temps_plein"] = 1

        # nb_enf
        df["nenf"] = 1
        df.at[df["nb_enf"] == "00", "nenf"] = 0

        # salaire
        df["salaire"] = 0
        df.salmt = df.salmt.apply(lambda x: x.split(",")[0])
        df.salmt = df.salmt.astype('int')
        df.at[(df["salunit"] == "H"), "salaire"] = df["salmt"] * 40 * 4 * 12
        df.at[(df["salunit"] == "M"), "salaire"] = df["salmt"] * 12
        df.at[df["salaire"] > 100000, "salaire"] = 100000

        # resqpv
        df["resqpv"] = 0
        df.at[df.qpv.notnull(), "resqpv"] = 1

        # dipl
        df["dipl"] = 0
        df.at[df["diplome"] == "D", "dipl"] = 1

        # matrimon
        df["matrimon"] = 0
        df.at[(df["sitmat"] == "C") | (df["sitmat"] == "V"), "matrimon"] = 1
        df.at[df["sitmat"] == "D", "matrimon"] = 2
        df.at[df["sitmat"] == "M", "matrimon"] = 3

        # axe_trav
        df["axetrav"] = 0
        df.at[df["axe_trav"] == "F", "axetrav"] = 1
        df.at[df["axe_trav"] == "M", "axetrav"] = 2

        # cat_reg
        df = df[df.cat_reg.notnull()]

        # conssms
        df["sms"] = 0
        df.at[(df["conssms"] == "N"), "sms"] = 1

        # consmail
        df["mail"] = 0
        df.at[(df["consmail"] == "N"), "mail"] = 1

        # montant_indem
        df.montant_indem = df.montant_indem.astype(float)
        df.at[(df["montant_indem"] > 50000), "montant_indem"] = 50000
        df.at[df.montant_indem.isnull(), "montant_indem"] = 0

        # duree_indem
        df.duree_indem = df.duree_indem.astype(float)
        df.at[(df["duree_indem"] > 1000), "duree_indem"] = 1000
        df.at[df.duree_indem.isnull(), "duree_indem"] = 0

        # montant_indem_j
        df["montant_indem_j"] = 0
        df.at[df["duree_indem"] > 0.01, "montant_indem_j"] = df.montant_indem[
            df["duree_indem"] > 0.01] / df.duree_indem[df["duree_indem"] > 0.01]
        df["montant_indem_j"] = df["montant_indem_j"].round(0).fillna(0)
        df.at[df["montant_indem_j"] > 150, "duree_indem"] = 150

        # montant_indem_q
        df["montant_indem_q"] = 0
        df.at[df["montant_indem"] < 7400, "montant_indem_q"] = 1
        df.at[(df["montant_indem"] >= 7400) &
                (df["montant_indem"] < 15600), "montant_indem_q"] = 2
        df.at[(df["montant_indem"] >= 15600) &
                (df["montant_indem"] < 25500), "montant_indem_q"] = 3
        df.at[df["montant_indem"] >= 25500, "montant_indem_q"] = 4

        # ctp
        df["isctp"] = 0
        df.at[df.ctp.notnull(), "isctp"] = 1

        # entrep
        df["isentrep"] = 0
        df.at[df.entrep.notnull(), "isentrep"] = 1

        # h_trav
        df.h_trav = df.h_trav.astype(float)
        df.h_trav = df.h_trav.fillna(0)
        df.at[df["h_trav"] < 1, "h_trav"] = 0
        df.at[(df["h_trav"] >= 1) & (df["h_trav"] < 78), "h_trav"] = 1
        df.at[(df["h_trav"] >= 78) & (df["h_trav"] < 120), "h_trav"] = 2
        df.at[df["h_trav"] >= 120, "h_trav"] = 3

        # s_trav
        df.at[df.s_trav.isnull(), "s_trav"] = "0.0"

        # score_forma_diag
        df["score_forma_diag"] = df["score_forma_diag"].astype(float).round()
        df.at[df.score_forma_diag.isnull(), "score_forma_diag"] = 0

        # dep
        df["dep"] = df.depcom.apply(lambda x: str(x)[:2])
        df.at[df.dep.isin(('2A', '2B')), "dep"] = '20'
        # find na values in dep
        df = df[~df.dep.isin(('na', '98', '99'))]

        # clean nreg
        for dep in list(set(df.dep)):
            most_commun_reg = Counter(df[df.dep == dep].nreg).most_common()[0][0]
            df.at[df["dep"] == dep, "nreg"] = most_commun_reg

        # Drop col to drop
        df = df.drop(col_to_drop, axis=1)
        df = df.dropna()
        print("Preprocess done in {}".format((time() - start) / 60))

        return df
