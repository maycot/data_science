# -*- coding: utf-8 -*-

"""
Build the datasets for the model : 
   - target.csv for the input word (the word at the center of the window)
   - context.csv for the context word (the word in the context of the input word)
"""

import os
import pandas as pd

def create_target_context_csv(df, window_size, data_dir):
    """Build the training datasets : target.csv, context.csv
    ---
    Args:
       df : (pd.DataFrame) df with col of normalized text. 
            The col name must be "libelle" 
       window_size : (int) size of the 'context' ie the window 
                     of words to the left and to the right of
                     a target word.  
    """
    target = []
    context = []

    for el in df.libelle:

        words_list = el.split(" ")
        L = len(words_list)

        for index, word in enumerate(words_list):
            s = index - window_size
            e = index + window_size + 1

            for i in range(s, e):
                if i != index and 0 <= i < L:
                    target.append(word)
                    context.append(words_list[i])

    pd.DataFrame(target).to_csv(os.path.join(data_dir, 
                                'target_' + str(window_size) + '.csv'),
                                header=False, index=False)
    print('target.csv rows number : {}'.format(len(target)))
    pd.DataFrame(context).to_csv(os.path.join(data_dir, 
                                'context_' + str(window_size) + '.csv'),
                                header=False, index=False)
    print('context.csv rows number : {}'.format(len(context)))


if __name__ == '__main__':
    data_dir = '../data'
    corpus_filename = 'all_libelle_to_embed.txt'
    window_size = 2
    df = pd.read_csv(os.path.join(data_dir, corpus_filename))
    df.dropna(inplace=True)
    create_target_context_csv(df, window_size, data_dir)
