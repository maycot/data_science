# -*- coding: utf-8 -*-

"""
Build the 'libelle structure' corpus.
Mapping between:
   - code competence (from file dc_codecompetence_corpus.csv from get_data.sql) 
   - libelle competence, libelle_regroupement_competence_n2 and
     libelle_regroupement_competence_n1 from tim_ref_competence.csv
Result : dataset_dc_codecompetence.csv + tim_ref_competence.csv 
         ==> competences_structurees_corpus.csv
"""

import os
import pandas as pd

from utils import load_csv_to_df, dump_df_to_csv

def make_competences_structurees_corpus(data_dir, ref_filename,
                                        codes_competences_filename):

  ref_df = load_csv_to_df(data_dir, ref_filename)
  ref_df.drop(["regroupement_competence_n2", "regroupement_competence_n1"],
              axis=1, inplace=True)
  code_competence_df = pd.read_csv(os.path.join(data_dir, 
                                   codes_competences_filename),
                                   sep='|', dtype='unicode')
  competences_structurees_corpus_df = code_competence_df.merge(ref_df,
                                                               left_on="dc_codecompetence",
                                                               right_on="id_competence")
  competences_structurees_corpus_df.drop(['id_competence'],
                                         axis=1, inplace=True)
  competences_structurees_corpus_df.to_csv(os.path.join(data_dir,
                 'competences_structurees_corpus.csv'), sep='|')

if __name__ == '__main__':
  make_competences_structurees_corpus('../data', 
                                      'tim_ref_competence.csv',
                                      'dc_codecompetence_corpus.csv')