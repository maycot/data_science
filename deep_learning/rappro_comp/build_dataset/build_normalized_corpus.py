# -*- coding: utf-8 -*-

"""
Normalize the 2 corpus : 
competences_structurees_corpus.csv and
competences_libres_corpus.csv
"""

import os
import pandas as pd
from utils import (build_normalized_text_file,
                   build_dictionary_file)


def normalize_corpus(data_dir, f1, f2):

    # Create competences structurees corpus
    df1 = pd.read_csv(os.path.join(data_dir, f1), sep='|')
    # The text is in columns : libelle_competence, libelle_regroupement_competence_n2,
    # and libelle_regroupement_competence_n1
    col_list = [el for el in df1.columns if 'libelle' in el]
    build_normalized_text_file(df1, col_list, data_dir, 
                               'norm_competences_structurees_corpus.txt')
    
    # Create competences libres corpus
    df2 = pd.read_csv(os.path.join(data_dir, f2), sep='\t')
    df2.dropna(inplace=True)
    df2.dc_libellelibre = df2.dc_libellelibre.apply(lambda x: x.split('\n')[0])
    col_list = [el for el in df2.columns if 'libelle' in el]
    build_normalized_text_file(df2, col_list, data_dir,
                               'norm_competences_libres_corpus.txt')

if __name__ == '__main__':
  normalize_corpus('../data', 'competences_structurees_corpus.csv',
                   'competences_libres_corpus.csv')