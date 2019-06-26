# -*- coding: utf-8 -*-

"""
Vectorize all_libelle corpus with Term SUM
"""

import os
import pandas as pd
import numpy as np


def main(data_dir, corpus_filename1, corpus_filename2, dictionary_filename,
         model_dir, embedding_matrix_filename):
    
    # Word embedding DF with word as index
    vocab_df = pd.read_csv(os.path.join(data_dir, dictionary_filename),
                           header=None)
    vocab_df.columns = ['word']
    emb_matrix = pd.read_csv(os.path.join(model_dir, embedding_matrix_filename),
                             sep='\t', header=None)
    word_vector_df = vocab_df.join(emb_matrix)
    word_vector_df.set_index('word', inplace=True)

    # Libelle DF
    libelle_df1 = pd.read_csv(os.path.join(data_dir, corpus_filename1))
    libelle_df2 = pd.read_csv(os.path.join(data_dir, corpus_filename2))
    libelle_df = libelle_df1.append(libelle_df2)
    libelle_df.reset_index(inplace=True, drop=True)
    # Delete word duplicata in libelle
    libelle_df['libelle_set'] = libelle_df.libelle.apply(
        lambda x: list(set(x.split(" "))))

    # Build libelle vector: word sum
    libelle_vector_df = libelle_df.libelle_set.apply(lambda x: 
                                                     np.sum(word_vector_df.loc[x]))
    libelle_vector_df = libelle_df.join(libelle_vector_df)
    libelle_vector_df.to_csv(os.path.join(model_dir,
                             'libelle_vector_with_term_sum_2.csv'),
                             index=False)
    

if __name__ == '__main__':
    data_dir = '../data'
    model_dir = '../experiments/base_model_w3_e100'
    corpus_filename1 = 'test_sample_size50_2.csv'
    corpus_filename2 = 'test_sample_structure.csv'
    dictionary_filename = 'norm_competences_structurees_dictionary.txt'
    embedding_matrix_filename = 'normalized_embedding_matrix.tsv'
    main(data_dir, corpus_filename1, corpus_filename2, dictionary_filename,
         model_dir, embedding_matrix_filename)