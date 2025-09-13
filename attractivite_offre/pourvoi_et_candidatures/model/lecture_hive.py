# -*- coding: utf-8 -*-

"""
Fonction qui va chercher les données des datasets vues et candidatures
dans les tables du lac :
fb00_rda06200.dataset_vues_pe
fb00_rda06200.dataset_cdt_pe
"""
import os
from datetime import date, timedelta
from pyhive import hive
import pandas as pd
from utils.dataset_functions import dump_df_to_csv

DATA_DIR = './data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

CONNEXION_SQL = hive.Connection(host="hp1edge02.pole-emploi.intra", port=10010,
                                auth='KERBEROS', kerberos_service_name="hive")

def main():

    for el in ['offres', 'cdt']:
        filename = 'lac_' + el + '_predict.csv'
        table = 'fb00_rda06200.' + el + '_predict'
        ordre_sql = "SELECT * FROM " + table
        df = pd.read_sql(ordre_sql, CONNEXION_SQL)

        dump_df_to_csv(df, DATA_DIR, filename)

if __name__ == '__main__':
    main()
