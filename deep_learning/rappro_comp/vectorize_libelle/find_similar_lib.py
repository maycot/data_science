# -*- coding: utf-8 -*-

"""
Find most similar libelle
"""
import os
import json
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

model_dir = '../experiments/base_model_w3_e100'
filename = 'libelle_vector_with_term_sum_2.csv'
libelle_json_path = os.path.join(model_dir, 'results_term_sum_2.json')

df = pd.read_csv(os.path.join(model_dir, filename))

df_libre = df[df.label == 1]
df_structure = df[df.label == 0] 

df_libre.drop(['libelle', 'label', 'libelle_set'], axis=1, inplace=True)
df_structure.drop(['libelle', 'label', 'libelle_set'], axis=1, inplace=True)

def main():

    libelle_res = {}

    for i in range(len(df_libre)):
        lib = df_libre.iloc[i, 0]
        lib_vector = df_libre.iloc[i, 1:]
        similarity = cosine_similarity(lib_vector.values.reshape(1, -1),
                                       df_structure.iloc[:, 1:].values)
        nearest = (-similarity.flatten()).argsort()[1:4]
        nearest_lib = df_structure.iloc[nearest, 0].values
        nearest_similarity = np.round(similarity.flatten()[nearest], 3)
        result = list(zip(nearest_lib, nearest_similarity))
        libelle_res[lib] = [el[0] for el in result if el[1] >= 0.65]

    with open(libelle_json_path, 'w') as f:
        json.dump(libelle_res, f, indent=4)

if __name__ == '__main__':
    main()
