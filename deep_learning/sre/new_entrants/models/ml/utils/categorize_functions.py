# -*- coding: utf-8 -*-

""" Functions used in categorize.py to compute some features
"""

import pandas as pd

def ident_count_by_month(df, cols_to_groupby_list):
    """Count ident by combinations of features
    
    Parameters :
    ---
    cols_to_groupby_list (list) ex: ["dep", "rome"]
    """
    grouped = df.groupby(cols_to_groupby_list)
    ident_count = grouped["ident"].count().reset_index()
    ident_count.rename(columns={"ident": "lag0_ident_count"}, 
                       inplace=True)
    
    return ident_count


def ict1_count_by_month(df, cols_to_groupby_list):
    """Count ict1=1 by combinations of features
    
    Parameters :
    ---
    cols_to_groupby_list (list) ex: ["dep", "rome"]
    """
    grouped = df.groupby(cols_to_groupby_list)
    ict1_count = grouped["ict1"].sum().reset_index()
    ict1_count.rename(columns={"ict1": "lag0_ict1_count"},
                      inplace=True)
    
    return ict1_count


def recod_loc_entity(df, loc, entity):
    """Encode entity by ict1 ranking within the locality
    
    Parameters :
    ---
    loc (string) ex: "nreg"
    entity (string) ex: "dep"
    """
    loc_entity_recod_df = pd.DataFrame()
    first_year_of_dataset = df.year.min()
    grouped = df[df.year == first_year_of_dataset].groupby(
        [loc, entity])["ict1"]
    
    ict1_loc_entity_ratio = grouped.sum() / grouped.count()
    ict1_loc_entity_ratio = ict1_loc_entity_ratio.reset_index()
    
    for locality in set(ict1_loc_entity_ratio[loc]):
        loc_part = ict1_loc_entity_ratio[
            ict1_loc_entity_ratio[loc] == locality].sort_values(
            "ict1", ascending=False)
        entity_recod = list(range(1, len(loc_part) + 1))
        entity_recod = entity_recod[::-1]
        loc_part[loc + "_" + entity + "_recod"] = entity_recod
        loc_entity_recod_df = loc_entity_recod_df.append(loc_part)
    loc_entity_recod_df.drop(["ict1"], axis=1, inplace=True)

    return loc_entity_recod_df


def recod_entity(df, entity):
    """Encode entity by ict1 ranking
    
    Parameters :
    ---
    entity (string) ex: "dep"
    """
    entity_recod_df = pd.DataFrame()
    first_year_of_dataset = df.year.min()
    grouped = df[df.year == first_year_of_dataset].groupby(
        [entity])["ict1"]
    ict1_entity_ratio = grouped.sum() / grouped.count()
    ict1_entity_ratio = ict1_entity_ratio.sort_values(
        ascending=False).reset_index()
    
    recod = list(range(1, len(ict1_entity_ratio) + 1))
    entity_recod_df[entity + "_recod"] = recod[::-1]
    entity_recod_df[entity] = ict1_entity_ratio[entity]
    
    return entity_recod_df