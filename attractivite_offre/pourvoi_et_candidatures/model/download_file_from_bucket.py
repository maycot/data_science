# -*- coding: utf-8 -*-

"""
Télécharge du bucket s3 des fichiers utiles au preprocess:
    environnement_geographique_2019.csv
    commune_rech_counter.csv
    persp_2_rome_dep.csv
    de1_rome_be19.csv
"""

from utils.bucket_functions import get_file

FILES_LIST = [
    'environnement_geographique_2019.csv',
    'commune_rech_counter.csv',
    'persp_2_rome_dep.csv',
    'de1_rome_be19.csv',
    'dep_rech_counter.csv',
    'dataset_pourvoi_train_2019.csv',
    'dataset_pourvoi_train_2020.csv',
    'dataset_cdt_train.csv'
    ]

def main():
    for f in FILES_LIST:
        get_file('./data/' + f, f)
        print(f)

if __name__ == '__main__':
    main()