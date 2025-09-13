# -*- coding: utf-8 -*-

"""
Fonction principale pour lancer le model "attractivité vues et candidatures"

    1. Pourvoi :
        - Preprocessing : pourvoi/make_dataset.py - > Création du dataset dataset_pourvoi_predict.csv
        - Modèle : pourvoi/main.py

    2. Candidatures :
        - Preprocessing : candidatures/make_dataset.py - > Création du dataset dataset_cdt_predict.csv
        - Modèle: candidatures/main.py
"""
import os
# import lecture_hive
import download_file_from_bucket
import pourvoi.make_dataset as pourvoi_preprocess
import pourvoi.main as pourvoi_model
import candidatures.make_dataset as candidatures_preprocess
import candidatures.main as candidatures_model

if __name__ == '__main__':
    #download_file_from_bucket.main()
    # lecture_hive.main()
    # pourvoi_preprocess.main()
    # pourvoi_model.main()
    candidatures_preprocess.main()
    #candidatures_model.main()
