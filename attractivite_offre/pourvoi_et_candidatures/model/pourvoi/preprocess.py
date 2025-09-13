# -*- coding: utf-8 -*-

""" Preprocess data (step 1) : Clean and categorize
"""
import random
from datetime import date
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
        """Pour le train       """
        df = df[~df.delai_vie.isnull()]
        df.delai_vie = df.delai_vie.astype(int)
        df = df[df.delai_vie >= 0]
        df['dom_pro'] = df.dc_rome_id.apply(lambda x: x[:3])
        rome_to_drop_mask = df.groupby('dc_rome_id').kc_offre.count() < 20
        rome_to_drop = rome_to_drop_mask[rome_to_drop_mask].index.tolist()
        appelrome_to_drop_mask = df.groupby('dc_appelationrome_id').kc_offre.count() < 20
        appelrome_to_drop = appelrome_to_drop_mask [appelrome_to_drop_mask].index.tolist()
        df = df[~df.dc_rome_id.isin(rome_to_drop)]
        df = df[~df.dc_appelationrome_id.isin(appelrome_to_drop)]
        df = df.fillna('0')
        df = df[df.contrat.isin(('0', 'mer_plus', 'dpae', 'gaec'))]
        df.at[df['delai_vie'] > 200, 'delai_vie'] = 200
 
        df['dom_pro'] = df.dc_rome_id.apply(lambda x: x[:3])
        df = df.fillna('0')
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
        df.at[df.dc_typexperienceprof_id.isnull(),
              'dc_typexperienceprof_id'] = 'E'

        # format datetime and make mois and annee features
        df = df[df.dd_datecreationreport.notnull()]
        df.dd_datecreationreport = pd.to_datetime(
            df.dd_datecreationreport.apply(lambda x: x[:10]))
        df['mois'] = df.dd_datecreationreport.apply(lambda x: x.month)
        df['annee'] = df.dd_datecreationreport.apply(lambda x: x.year)
        df['wd'] = df.dd_datecreationreport.apply(lambda x: x.weekday())

        df.at[df['dc_modepreselection_id'].isin(('M', 'P')),
              'dc_modepreselection_id'] = 'C'

        df.at[df['dc_unitedureecontrat'] != 'JO', 'dc_unitedureecontrat'] = 'MO'

        df = df[df.dc_categorie_contrat.isin(('CDI', '1-3', '>6', '3-6', '<1'))]

        df = df[df.dc_topalertqltoffpremverif.isin(('N', 'S'))]
        df = df[df.dc_topalertqltoffextraction.isin(('N', 'S'))]

        df.at[(df['dn_statutetablissement'] == '9') |
              (df['dn_statutetablissement'] == '18'), 'dn_statutetablissement'] = '10'

        df['dep'] = df['dc_communelieutravail'].apply(lambda x: x[:2])
        df = df[df.dep != 97]

        df['is_cdi'] = 0
        df.at[df.dc_typecontrat_id == 'CDI', 'is_cdi'] = 1

        df['naf3'] = df.naf.apply(lambda x: x[:3])
        df['is_interim'] = 1
        df.at[df.naf3 == '782', 'is_interim'] = 0

        for el in ['dc_topalertqltoffpremverif', 'dc_topalertqltoffextraction']:
            df.at[df[el] == 'N', el] = 0
            df.at[df[el] == 'S', el] = 1

        df.at[df['dc_topdesquepossible'] == 'N', 'dc_topdesquepossible'] = 1
        df.at[df['dc_topdesquepossible'] == 'O', 'dc_topdesquepossible'] = 0

        df.at[df['dc_typesalaire'] != 'H', 'dc_typesalaire'] = 1
        df.at[df['dc_typesalaire'] == 'H', 'dc_typesalaire'] = 0

        df.at[df['dc_typexperienceprof_id'] == 'S', 'dc_typexperienceprof_id'] = 0
        df.at[df['dc_typexperienceprof_id'] == 'D', 'dc_typexperienceprof_id'] = 1
        df.at[df['dc_typexperienceprof_id'] == 'E', 'dc_typexperienceprof_id'] = 3

        df.at[df['dc_typecontrat_id'] == 'MIS', 'dc_typecontrat_id'] = 0
        df.at[df['dc_typecontrat_id'] == 'CDD', 'dc_typecontrat_id'] = 1
        df.at[df['dc_typecontrat_id'] == 'autre', 'dc_typecontrat_id'] = 2
        df.at[df['dc_typecontrat_id'] == 'CDI', 'dc_typecontrat_id'] = 3

        df.at[df['dc_typeaffichage_id'] != 'N', 'dc_typeaffichage_id'] = 1
        df.at[df['dc_typeaffichage_id'] == 'N', 'dc_typeaffichage_id'] = 0

        df.at[df['dc_unitedureecontrat'] != 'JO', 'dc_unitedureecontrat'] = 1
        df.at[df['dc_unitedureecontrat'] == 'JO', 'dc_unitedureecontrat'] = 0

        df.at[df['dc_categorie_contrat'] == '<1', 'dc_categorie_contrat'] = 0
        df.at[df['dc_categorie_contrat'] == '1-3', 'dc_categorie_contrat'] = 1
        df.at[df['dc_categorie_contrat'] == '3-6', 'dc_categorie_contrat'] = 2
        df.at[df['dc_categorie_contrat'] == '>6', 'dc_categorie_contrat'] = 3
        df.at[df['dc_categorie_contrat'] == 'CDI', 'dc_categorie_contrat'] = 4

        df.at[~df['dc_categorie_experience'].isin(('<1', 'debutant')),
              'dc_categorie_experience'] = 1
        df.at[df['dc_categorie_experience'].isin(('<1', 'debutant')),
              'dc_categorie_experience'] = 0

        df.at[~df['dc_categorie_dureetravailhebdoheures'].isin(('24-35', '<24')),
              'dc_categorie_dureetravailhebdoheures'] = 2
        df.at[df['dc_categorie_dureetravailhebdoheures'] == '<24',
              'dc_categorie_dureetravailhebdoheures'] = 0
        df.at[df['dc_categorie_dureetravailhebdoheures'] == '24-35',
              'dc_categorie_dureetravailhebdoheures'] = 1

        df.at[df['dc_topinternet'] == '1', 'dc_topinternet'] = 0
        df.at[df['dc_topinternet'] == '0', 'dc_topinternet'] = 1

        df.at[df['dc_acteur_miseajour1_id'] == 'SY', 'dc_acteur_miseajour1_id'] = 0
        df.at[df['dc_acteur_miseajour1_id'] == 'CO', 'dc_acteur_miseajour1_id'] = 1
        df.at[df['dc_acteur_miseajour1_id'] == 'IE', 'dc_acteur_miseajour1_id'] = 2
        df.at[df['dc_acteur_miseajour1_id'] == '0', 'dc_acteur_miseajour1_id'] = 3
        """ TRAIN
        df['delta_day'] = df.apply(lambda x: random.randint(0, min(x['delai_vie'], 14)), axis=1)
        """
        today_date = pd.to_datetime(date.today())
        df['delta_day'] = df.dd_datecreationreport.apply(lambda x: (today_date - x).days)
        df = df[df.delta_day < 15]

        return df
