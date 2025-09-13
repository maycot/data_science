# -*- coding: utf-8 -*-

"""
Envoi dans le bucket s3 des fichiers utiles au preprocess:
    environnement_geographique_2019.csv
    commune_rech_counter.csv
    persp_2_rome_dep.csv
    de1_rome_be19.csv
"""

from utils.bucket_functions import send_file

FILES_LIST = [
    'environnement_geographique_2019.csv',
    'commune_rech_counter.csv',
    'persp_2_rome_dep.csv',
    'de1_rome_be19.csv',
    'dep_rech_counter.csv'
    ]

def main():
    for f in FILES_LIST:
        send_file('./data/' + f, f)
        print(f)

if __name__ == '__main__':
    main()