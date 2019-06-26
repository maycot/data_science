# -*- coding: utf-8 -*-

"""
Build the "All libelle" corpus with : 
- libelle from norm_competences_structurees_corpus
- libelle from norm_competences_libres_corpus after cleaning with map_dict
"""

import os
import pandas as pd
import numpy as np
import json
from pprint import pprint
from sklearn.utils import shuffle
from nltk.tokenize import wordpunct_tokenize

def tokenize_libelle(libelle):
    """Tokenize libelle in list of tokens.
    """
    lib = wordpunct_tokenize(libelle)
    lib = [el for el in lib if len(el) > 2 and el not in ('nan', 'null')]
    return lib


def replace_word(libelle_list, map_dict):
    """Replace words in libelle_list with values of map_dict.
    """
    for i, word in enumerate(libelle_list):
            libelle_list[i] = map_dict[word]
    return libelle_list


def build_all_libelle_corpus(data_dir, comp_structurees_df, comp_libres_df,
                             map_dict):
    """ Build all libelle_corpus.
    """
    # Build libelle col for competences libres
    comp_libres_df['libelle'] = comp_libres_df['libelle'].apply(lambda x: tokenize_libelle(x))
    comp_libres_df['libelle'] = comp_libres_df['libelle'].apply(lambda x: replace_word(x, map_dict))
    comp_libres_df['libelle'] = comp_libres_df['libelle'].apply(lambda x: [item for sublist in x
                                                                for item in sublist])
    comp_libres_df['libelle'] = comp_libres_df['libelle'].apply(lambda x: " ".join(x))
    comp_libres_df['label'] = 1

    # Build libelle col for competences structurees
    comp_structurees_df['libelle'] = comp_structurees_df['libelle'].apply(lambda x: tokenize_libelle(x))
    comp_structurees_df['libelle'] = comp_structurees_df['libelle'].apply(lambda x: " ".join(x))
    comp_structurees_df['label'] = 0

    # Build all libelle DF
    all_libelle_corpus = comp_structurees_df[['real_libelle', 'libelle', 'label']].append(
        comp_libres_df[['real_libelle', 'libelle', 'label']])
    print('File all_libelle_corpus has {} lib.'.format(len(all_libelle_corpus)))
    all_libelle_corpus.to_csv(os.path.join(data_dir, 'all_libelle_corpus.csv'), index=False)

    all_libelle_to_embed = all_libelle_corpus[['libelle']].drop_duplicates()
    print('Number of duplicata in lib : {}'.format(len(all_libelle_corpus) - len(all_libelle_to_embed)))
    all_libelle_to_embed.dropna(inplace=True)
    all_libelle_to_embed = shuffle(all_libelle_to_embed, random_state=10)
    print('File all_libelle_to_embed has {} lib.'.format(len(all_libelle_to_embed)))
    all_libelle_to_embed.to_csv(os.path.join(data_dir, 'all_libelle_to_embed.txt'), index=False)

def main():
    # Load DF
    data_dir = '../data'
    comp_libres_filename = 'competences_libres_corpus_revers.csv'
    comp_structurees_filename = 'competences_structurees_corpus_revers.csv'

    comp_libres_df = pd.read_csv(os.path.join(data_dir, comp_libres_filename))
    comp_structurees_df = pd.read_csv(os.path.join(data_dir, comp_structurees_filename))

    # Load map_dict
    with open(os.path.join(data_dir, 'map_dict.json')) as f:
        map_dict= json.load(f)
    
    build_all_libelle_corpus(data_dir, comp_structurees_df, comp_libres_df, map_dict)

if __name__ == '__main__':
    main()


