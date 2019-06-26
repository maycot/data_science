# -*- coding: utf-8 -*-

"""
Utility functions for text processing
"""

import os
import re
import unicodedata
import pandas as pd
from string import punctuation

import nltk
from nltk.corpus import stopwords
from nltk.stem.snowball import FrenchStemmer
from nltk.tokenize import wordpunct_tokenize
from nltk.probability import FreqDist


def normalize_text(string):
    """Preprocess text string to return a normalized form of the text.
    """
    if isinstance(string, float):
        return ""
    else:
        # lowering x, removing beginning and ending space
        s = string.strip().lower()

    # removing accents
    s =''.join((c for c in unicodedata.normalize('NFD', s)
                if unicodedata.category(c) != 'Mn'))
    
    # remove punctuation
    s = re.sub("["+punctuation+"]", " ", s)
    
    # remove uninformative, stop words and non alpha words
    words_to_remove = ["les", "une", "des", "nos", "ils", 
                       "elle", "elles", "nan", "null"]
    stop_words = list(stopwords.words("french"))
    remove_list = words_to_remove + stop_words
    s = " ".join([word for word in s.split() if
                  (word.isalpha() and
                   word not in remove_list and
                   len(word) > 2)])

    # Stemming words and remove duplicates
    stemmer = FrenchStemmer()
    stem_words = [stemmer.stem(w) for w in s.split()]
    s = " ".join(stem_words)

    return s


def build_normalized_text_file(df, col_list, data_dir, corpus_filename):
    """
    Create a txt file with lines of text normalized
    with normalize_text.
    ---
    Args:
       df : (pd.DataFrame) df with col of text
       col_list : (list) list of columns with text to merge in one text
    """

    df["concat_text"] = ""
    for col in col_list:
        df[col] = df[col].apply(lambda x: str(x) + " ")
        df["concat_text"] = df["concat_text"] + df[col]
    
    # normalize text in concat_text col
    df["concat_text"] = df["concat_text"].transform(normalize_text)
    df["concat_text"].to_csv(os.path.join(data_dir, corpus_filename),
                             index=False)


def build_dictionary_file(data_dir, corpus_filename, dict_filename):
    """Create a dictionary txt file from corpus text file
    """
    corpus_df = pd.read_csv(os.path.join(data_dir, corpus_filename),
                            header=None)
    # Create one unique texte
    all_lines_text = ' '.join(corpus_df.loc[:, 0].dropna().values)
    # Create dictionary
    dico = list(set(wordpunct_tokenize(all_lines_text)))
    dico = [el for el in dico if len(el) > 1]
    
    pd.Series(dico).to_csv(os.path.join(data_dir, dict_filename),
                           index=False)


def load_csv_to_df(path, filename):
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep='|', dtype='unicode',
                     error_bad_lines=False)
    df.columns = [el.split(".")[1] for el in df.columns]
    return df


def dump_df_to_csv(df, path, filename):
    """Dump pandas df to csv file.
    """
    df.to_csv(os.path.join(path, filename), index=False, sep='|')
