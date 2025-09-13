# -*- coding: utf-8 -*-

"""
Fonction principale pour lancer le modèle ciblage candidats sur une liste de rome
"""
import pandas as pd
from preprocess import main as preprocess_main
from model_2.classification_train import main as classifier_main2
from model_3.classifier import main as classifier_main3
from model_4.classifier import main as classifier_main4

CODE_ROME_LIST = [
    'N1103',
    'K1302',
    'D1401',
    'D1106',
    #'M1805'
]

def main():
    
    all_candidats_df = pd.DataFrame()
    for num in ['1', '2', '3']:
        for code_rome in CODE_ROME_LIST:
            print('\n', code_rome,'\n')
            train_filename = 'features' + num + '_2020_' + code_rome + '.csv'
            train_output_filename = 'train_dataset' + num + '_' + code_rome + '.csv'
            valid_filename = 'features' + num + '_2021_' + code_rome + '.csv'
            valid_output_filename = 'valid_dataset' + num + '_' + code_rome + '.csv'

            train_df = preprocess_main(train_filename, train_output_filename, code_rome)
            valid_df = preprocess_main(valid_filename, valid_output_filename, code_rome)
            candidats_df2 = classifier_main2(train_df, valid_df, code_rome)
            candidats_df3 = classifier_main3(train_df, valid_df, code_rome)
            candidats_df4 = classifier_main4(train_df, valid_df, code_rome)
            candidats_df = candidats_df4.copy()
            pred_sum = candidats_df2['pred'] + candidats_df3['pred'] + candidats_df4['pred']
            candidats_df['pred'] = 0
            candidats_df.at[pred_sum > 1, 'pred'] = 1
            candidats_df = candidats_df[candidats_df.pred == 1]
            candidats_df['mod'] = 3 - int(num)
            all_candidats_df = all_candidats_df.append(candidats_df)

    all_candidats_df.drop_duplicates(subset='kn_individu_national', inplace=True)
    all_candidats_df = all_candidats_df.sort_values(by=['rome', 'mod', 'predict_proba'], ascending=False)
    print(all_candidats_df.shape)
    all_candidats_df.to_csv('./data/model3_candidats.csv', index=False, sep='|')

if __name__ == '__main__':
    main()
