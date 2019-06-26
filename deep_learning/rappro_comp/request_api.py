# -*- coding: utf-8 -*-

"""
Request API to get list of skills
"""
import os
import json
import pandas as pd
import requests

data_dir = '../data'
corpus_filename = 'test_sample_size50_2.csv'
#code_json_path = os.path.join(data_dir, 'word_to_job_results_code.json')
libelle_json_file = 'word_to_job_results_libelle_2.json'

df = pd.read_csv(os.path.join(data_dir, corpus_filename))

skills_list = df.real_libelle.tolist()

url = ' '

def main():

    libelle_res = {}

    for skill in skills_list:
        params = {'libelle': skill}
        response = requests.get(url=url, params=params)
        data = response.json()
        libelle_res[skill] = [el['libelle'] for el in data['references']]

    with open(libelle_json_file, 'w') as f:
        json.dump(libelle_res, f, indent=4)

if __name__ == '__main__':
    main()
