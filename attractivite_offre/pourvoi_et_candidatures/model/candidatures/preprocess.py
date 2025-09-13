# -*- coding: utf-8 -*-

""" Preprocess data (step 1) : Clean and categorize
"""

from sys import exit
from datetime import date, timedelta
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class Preprocess(BaseEstimator, TransformerMixin):

    def __init__(self):
        pass

    def fit(self, df, y=None):
        return self

    def transform(self, df, y=None):

        df = df[((df.dc_modepreselection_id == 'C') &
                 (df.dc_modepresentation_agence_id == 'ACV') &
                 (df.dc_modepresentation_emp_id != 'URL')) |
                ((df.dc_modepreselection_id == 'A') &
                 (df.dc_modepresentation_emp_id == 'MEL'))]
        df = df[df.dd_datecreationreport.notnull()]
        df.dd_datecreationreport = pd.to_datetime(
            df.dd_datecreationreport.apply(lambda x: str(x)[:10]))
        date_max = max(df.dd_datecreationreport)
        if (date_max.weekday() >= 4 and date_max < (date.today() - timedelta(3))):
            print('##### ALERTE ##### \n')
            print('##### Données NON MAJ BREAK #####')
            exit()
        elif (date_max.weekday() < 4 and date_max < (date.today() - timedelta(1))):
            print('##### ALERTE ##### \n')
            print('##### Données NON MAJ BREAK #####')
            exit()
        df.at[df.cdt_jour.isnull(), 'jour'] = df.loc[df.cdt_jour.isnull(),
                                                     'dd_datecreationreport']
        df.cdt_jour = pd.to_datetime(df.cdt_jour, errors='coerce')
        df['cdt_delta_day'] = df.cdt_jour - df.dd_datecreationreport
        df['cdt_delta_day'] = df['cdt_delta_day'].apply(lambda x: x.days)
        df['cdt_count'] = df['cdt_count'].astype(float)

        # Add target cdt count
        df.cdt_count = df.cdt_count.fillna(0)
        df.cdt_delta_day = df.cdt_delta_day.fillna(0)
        total_cdt_df = df.groupby('kc_offre')[['cdt_count']].sum()
        total_cdt_df.columns = ['total_cdt']
        df = df.set_index('kc_offre').join(total_cdt_df)
        df = df.reset_index()
        df['cdt_count'] = df.sort_values(['kc_offre', 'cdt_delta_day']).groupby(
            'kc_offre')['cdt_count'].cumsum()

        # dc_typesalaire: C -> X
        df.at[df['dc_typesalaire'] == 'C', 'dc_typesalaire'] = 'X'

        # Categorize dc_typeformation_1_id in nivfor
        df['nivfor'] = 0
        df.at[df['dc_typeformation_1_id'] == 'AFS', 'nivfor'] = 1
        df.at[df.dc_typeformation_1_id.isin((
            'C12', 'C3A', 'CFG', 'CP4')), 'nivfor'] = 2
        df.at[df['dc_typeformation_1_id'] == 'NV5', 'nivfor'] = 3
        df.at[df['dc_typeformation_1_id'] == 'NV4', 'nivfor'] = 4
        df.at[df['dc_typeformation_1_id'] == 'NV3', 'nivfor'] = 5
        df.at[df['dc_typeformation_1_id'] == 'NV2', 'nivfor'] = 6
        df.at[df['dc_typeformation_1_id'] == 'NV1', 'nivfor'] = 7
        df = df.drop('dc_typeformation_1_id', axis=1)

        df.at[df['dc_typecontrat_id'] == 'DIN', 'dc_typecontrat_id'] = 'MIS'
        df.at[df['dc_typecontrat_id'].isin((
            'CDU', 'CDS', 'TTI', 'INT')), 'dc_typecontrat_id'] = 'CDD'
        df.at[~df['dc_typecontrat_id'].isin((
            'CDI', 'MIS', 'CDD')), 'dc_typecontrat_id'] = 'autre'
        # clean etab features
        df.at[df['dc_trancheeffectifetab'].isin((
            '42', '51', '52', '53')), 'dc_trancheeffectifetab'] = '42'
        # null values
        df.at[df.dc_typeservicerecrutement_1_id.isnull(),
              'dc_typeservicerecrutement_1_id'] = 'APP'
        df.at[df.dc_typexperienceprof_id.isnull(),
              'dc_typexperienceprof_id'] = 'E'

        # format datetime and make mois and annee features
        df['mois'] = df.dd_datecreationreport.apply(lambda x: x.month)
        df['wd'] = df.dd_datecreationreport.apply(lambda x: x.weekday())

        df['dc_idcdifficulteeconomique_id'] = df.dc_idcdifficulteeconomique_id.fillna(0)

        df.at[df['dc_modepreselection_id'].isin(('M', 'P')),
              'dc_modepreselection_id'] = 'C'

        df.at[df['dc_unitedureecontrat'] != 'JO', 'dc_unitedureecontrat'] = 'MO'

        df = df[df.dc_categorie_contrat.isin(('CDI', '1-3', '>6', '3-6', '<1'))]

        df = df[df.dc_topalertqltoffpremverif.isin(('N', 'S'))]
        df = df[df.dc_topalertqltoffextraction.isin(('N', 'S'))]

        df.at[(df['dn_statutetablissement'] == 9) |
              (df['dn_statutetablissement'] == 18), 'dn_statutetablissement'] = 10.0

        # add environmental features : be19, ict1, ict3, ict10, prin1, prin2, prin3
        env_df = pd.read_csv('./data/environnement_geographique_2019.csv', dtype='unicode')
        df = df.merge(env_df, how='left', left_on='dc_communelieutravail',
                      right_on='kc_commune')
        df = df.drop(['kc_commune', 'dc_departement', 'dc_region'], axis=1)
        df = df[df.be19.notnull()]
        df = df[df.ict1.notnull()]
        df['dep'] = df['dc_communelieutravail'].apply(lambda x: x[:2])
        df = df[df.dep != 97]

        # add smart emploi indicator
        smart_df = pd.read_csv('./data/de1_rome_be19.csv', sep=';', dtype='unicode')
        smart_df = smart_df[(smart_df.year == '2019') & (smart_df.sais == 'T4')]
        df = df.merge(smart_df, how='left', left_on=['dc_rome_id', 'be19'],
                      right_on=['rome', 'be19'])
        df['DE_ABC_rome_be19'].fillna(0, inplace=True)
        df.drop(['rome', 'sais', 'year'], axis=1, inplace=True)

        smart_df = pd.read_csv('./data/persp_2_rome_dep.csv', sep=';', dtype='unicode')
        df = df.merge(smart_df, how='left', left_on=['dc_rome_id', 'dep'],
                      right_on=['CODE_ACTIVITE', 'CODE_TERRITOIRE'])
        df.rename(columns={'VALEUR_PRINCIPALE_DOM': 'persp2'}, inplace=True)
        df['persp2'].fillna(0, inplace=True)
        df.drop(['CODE_ACTIVITE', 'CODE_TERRITOIRE'], axis=1, inplace=True)
        # add features on commune counter
        commune_rech_counter = pd.read_csv(
            './data/commune_rech_counter.csv', sep='|', dtype='unicode')
        df = df.merge(commune_rech_counter, how='left',
                      left_on=['dc_communelieutravail'], right_on=['commune_rech'])
        df['commune_rech_counter'].fillna(0, inplace=True)
        commune_50_list = commune_rech_counter.commune_rech.iloc[:50]
        df['is_commune_50'] = 0
        df.at[df.dc_communelieutravail.isin((commune_50_list)), 'is_commune_50'] = 1

        dep_rech_counter = pd.read_csv('./data/dep_rech_counter.csv', sep='|',
                                       dtype='unicode')
        df['dep_rech'] = df['dc_communelieutravail'].apply(lambda x: x[:2])
        df = df.merge(dep_rech_counter, how='left', on=['dep_rech'])
        dep_5_list = dep_rech_counter.dep_rech.iloc[:5]
        df['is_dep_5'] = 0
        df.at[df.dep_rech.isin((dep_5_list)), 'is_dep_5'] = 1
        dep_15_list = dep_rech_counter.dep_rech.iloc[:15]
        df['is_dep_15'] = 0
        df.at[df.dep_rech.isin((dep_15_list)), 'is_dep_15'] = 1

        df['is_cdi'] = 0
        df.at[df.dc_typecontrat_id == 'CDI', 'is_cdi'] = 1

        df['is_titre'] = 1
        df.at[df.dc_intituleoffre.isnull(), 'is_titre'] = 0

        df['is_interim'] = 0
        df.at[df.naf3 == 782, 'is_interim'] = 1

        df.at[df.cdt_jour.isnull(), 'cdt_jour'] = df.loc[df.cdt_jour.isnull(),
                                                         'dd_datecreationreport']

        df.cdt_jour = pd.to_datetime(df.cdt_jour, errors='coerce')
        df['delta_cdt_wd'] = df.cdt_jour.apply(lambda x: x.weekday())
        
        df = df[df.cdt_delta_day < 14]

        return df
