# -*- coding: utf-8 -*-

""" Preprocess data (step 1) : Clean and categorize
"""
import pandas as pd
import collections
from utils.dataset_functions import (load_csv_from_lac_to_df, dump_df_to_csv)

PATH = './data/'

NUMERICAL_FEAT_LIST = [
    'delta_dateinscriptionpec',
    'delta_previous_dateinscriptionpec',
    'h_trav_m',
    's_trav_m',
    'three_months_h_trav',
    'three_months_s_trav',
    'six_months_h_trav',
    'six_months_s_trav',
    'delta_last_periodeact',
    'periodeactgaec_total_count',
    'last_quantiteactivite',
    'pcs_counter',
    'naf_counter',
    'typepec_count',
    'ale_count',
    'numpec_count',
    'rome_profil_count',
    'ami_entrants_count',
    'montant_indem',
    'duree_indem',
    'delta_fin_indem',
    'ouverturedroit_count',
    'motif_fin_interim',
    'six_months_maladie_sum',
    'ami_entrants_count',
    'ouverturedroit_count',
    'six_months_evenmtperso_sum',
    'typeperiodegaec_total_count',
    'has_rome',
    'has_rome_proche',
    'last_typeperiodegaec',
    'last_quantiteactivite',
    'six_months_quantiteact_sum',
    'six_months_periodeact_count',
    'forma_count',
    'typeforma_count',
    'domforma_count',
]

def compute_comp_percent(df, label):
    df = df[df['label'] == label].drop(['kn_individu_national', 'label'], axis=1)
    df = df.values
    comp_list = []
    for i in range(len(df)):
        comp_list = comp_list + list(set(df[i]))
    comp_list_num = collections.Counter(comp_list)
    ind_count = len(df)
    for k, v in comp_list_num.items():
        comp_list_num[k] = v / ind_count
    df_comp_percent = pd.DataFrame(index=comp_list_num.keys(), data=comp_list_num.values())
    df_comp_percent.columns = ['percent']
    df_comp_percent = df_comp_percent.sort_values(by='percent', ascending=False).iloc[1:]
    df_comp_percent = df_comp_percent[df_comp_percent['percent'] > 0.02]
    return df_comp_percent


def main(input_filename, output_filename, code_rome):
    '''Preprocess data'''
    df = load_csv_from_lac_to_df(PATH, input_filename)
    df = df[(df.dn_toprechercheemploi == '1') &
            (df.dn_topstage == '0') &
            (df.dn_topmaladie == '0') &
            (df.dn_topmaternite == '0') &
            (df.dn_topretraite == '0') &
            (df.dn_topinvalidite == '0') &
            ((df.had_same_pcs == '1') |
             ((df.had_pcs == '1') & (df.rome_profil == code_rome)))]
    # competences
    comp_col = [el for el in df.columns if 'dc_specificite' in el]
    col_to_keep = []
    if 'train' in output_filename:
        col_to_keep = ['kn_individu_national', 'label']
    else:
        col_to_keep = ['kn_individu_national']
    comp_df = df[col_to_keep + comp_col]
    df = df.drop(comp_col, axis=1)
    comp_df = comp_df[comp_df.isnull().sum(axis=1) < 80]
    comp_list = comp_df.drop(col_to_keep, axis=1).values.flatten()
    comp_list = list(set(comp_list))

    comp_ref_df = pd.read_csv(PATH + 'comp_ref.csv', sep='|', dtype='str')
    comp_ref_df = comp_ref_df[['KC_COMPETENCESROME', 'KC_NOEUDCOMPETENCE']]
    comp_ref_dict = dict(comp_ref_df.values)

    for comp in comp_list:
        try:
            comp_df = comp_df.replace(comp, comp_ref_dict[comp])
        except:
            comp_df = comp_df.replace(comp, '0')
    
    if 'train' in output_filename:
        comp_percent_df1 = compute_comp_percent(comp_df, '1')
        comp_percent_df0 = compute_comp_percent(comp_df, '0')
        comp_percent_diff_1_0 = comp_percent_df1.percent - comp_percent_df0.percent
        comp_to_keep = comp_percent_diff_1_0.abs().sort_values(ascending=False).iloc[:15].index.tolist()
        pd.DataFrame(comp_to_keep).to_csv(PATH + code_rome + '_comp_to_keep.csv', sep='|', index=False)
    else:
        comp_to_keep = pd.read_csv(PATH + code_rome + '_comp_to_keep.csv', sep='|', dtype='str')
        comp_to_keep = comp_to_keep.values.flatten().tolist()
    
    # Competences features
    comp_array = comp_df.drop(col_to_keep, axis=1).values
    comp_array = [list(set(el)) for el in comp_array]
    for i in range(len(comp_array)):
        if '0' in comp_array[i]:
            comp_array[i].remove('0')
            
    comp_df['comp_list'] = comp_array
    for i in range(len(comp_array)):
        comp_array[i] = list(set(comp_array[i]).intersection(set(comp_to_keep)))
    comp_df['comp_list_inter'] = comp_array
    
    comp_df = comp_df[['kn_individu_national', 'comp_list', 'comp_list_inter']]
    def find_comp_profil(comp_list_item, i):
        try:
            return comp_list_item[i]
        except:
            return '0'
    for i in range(3):
        comp_df['comp_profil_' + str(i)] = comp_df['comp_list'].apply(lambda x: find_comp_profil(x, i))
    for i in range(len(comp_to_keep)):
        comp_df['comp_' + str(i)] = comp_df['comp_list_inter'].apply(
            lambda x: 1 if comp_to_keep[i] in x else 0)
    comp_df = comp_df.drop(['comp_list', 'comp_list_inter'], axis=1)

    df = df.set_index('kn_individu_national').join(comp_df.set_index('kn_individu_national'))
    df = df.reset_index()
    df = df.fillna(0)
    df.datins = df.datins.apply(lambda x: x[:10])
    df.at[df['last_typeperiodegaec'].isin(('15', '16', '7', '9')), 'last_typeperiodegaec'] = '1'
    df.at[df['last_typeperiodegaec'].isin(('10', '13', '14', '17', '18', '19', '2', '20', '21', '8')), 'last_typeperiodegaec'] = '111'
    df.at[df['last_typeperiodegaec'].isin(('11', '4')), 'last_typeperiodegaec'] = '11'
    df.at[df['last_typeperiodegaec'].isin(('12', '5', '6')), 'last_typeperiodegaec'] = '1111'
    df.at[~df['last_typeperiodegaec'].isin(('1', '11', '111')), 'last_typeperiodegaec'] = '0'
    df['age'] = df['datenais'].apply(lambda x: 2021 - int(x[:4]))
    df['has_rome'] = 0
    df.at[df.rome_profil == code_rome, 'has_rome'] = 1
    df.at[df['last_dipl'] == 0, 'last_dipl'] = '-'
    df['last_dipl'] = df['last_dipl'].str.lower()

    df.at[~df['dc_motifinscription_id'].isin(('15', '19', '14', '28', '39')),
          'dc_motifinscription_id'] = '00'
    df['motif_fin_interim'] = 0
    df.at[df['dc_motifinscription_id'] == '15', 'motif_fin_interim'] = 1
    df.at[~df['previous_typepec'].isin(('1', '2')), 'previous_typepec'] = '00'
    df.at[~df['previous_soustypepec'].isin(('1', '3', '0')),
          'previous_soustypepec'] = '00'
    df.at[~df['previous_motifinscription'].isin(('15', '19', '14', 0)),
          'previous_motifinscription'] = '00'
    df.at[~df['qualif'].isin(('6', '5', '3', '1')), 'qualif'] = '00'
    df.at[~df['nivfor'].isin(('NV5', 'NV4', 'NV3', 'NV2')), 'nivfor'] = '00'

    for el in NUMERICAL_FEAT_LIST:
        df[el] = df[el].astype(float).astype(int)
        if 'forma' in el:
            df.at[df[el] > 2, el] = 3
        else:
            df[el] = df[el].abs()
            q_90 = df[el].quantile(0.9)
            df.at[df[el] > q_90, el] = q_90
    df['is_interim'] = 'non'
    df.at[df['last_naf_lieutrav'] != df['last_naf_affect'], 'is_interim'] = 'oui'
    dump_df_to_csv(df, PATH, output_filename)
    
    return df
