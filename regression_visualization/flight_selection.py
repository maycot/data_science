import pandas as pd
from sklearn.ensemble import IsolationForest


# Step 1 : Sélection vols durée typique (cf notebook vol_typiques)
# 2 durées typiques : entre 40 et 80 min et entre 120 et 140 min
def select_time_flights(all_datas):
    """"Select flights with typical flight time"""

    df_flight_time = all_datas.pivot_table(index='filename', values='time', \
                                           aggfunc=lambda x: len(x) * 2 / 60)
    bins = [40, 80, 110, 140]
    group_names = ['40-80', '80-110', '110-140']
    cats = pd.cut(df_flight_time, bins=bins, labels=group_names)
    index_flights_40_80 = cats[cats == '40-80'].index
    index_flights_110_140 = cats[cats == '110-140'].index
    df_flights = all_datas.set_index('filename'). \
        ix[index_flights_40_80.append(index_flights_110_140)]

    return df_flights


# Step 2 : Extraction des variables caractéristiques d'un vol typique
# For each of these variables "altitude, outside temp, ground speed"
# we take the mean, max, std

def select_typical_flights(df_flights):
    "Compute outsider flights with algorithme Isolation Forest on characteristic features"

    liste_var = ['altitude', 'outside temp', 'ground speed']
    for var in liste_var:
        df_flights[[var]] = df_flights[[var]].astype(float)

    liste_var = ['altitude', 'outside temp', 'ground speed']
    mean = {var: df_flights.reset_index().pivot_table(index='filename', values=var, aggfunc='mean') \
            for var in liste_var}
    maxi = {var: df_flights.reset_index().pivot_table(index='filename', values=var, aggfunc='max') \
            for var in liste_var}
    std = {var: df_flights.reset_index().pivot_table(index='filename', values=var, aggfunc='std') \
           for var in liste_var}

    df_var_caract = pd.concat([mean['altitude'], maxi['altitude'], std['altitude'],
                               mean['outside temp'], maxi['outside temp'], std['outside temp'],
                               mean['ground speed'], maxi['ground speed'], std['ground speed']], axis=1)

    df_var_caract.columns = ['alt_mean', 'alt_max', 'alt_std', 'outsT_mean', 'outsT_max', 'outsT_std', \
                             'grSpeed_mean', 'grSpeed_max', 'grSpeed_std']

    # Algo Isolation Forest
    clf = IsolationForest(n_estimators=100, max_samples='auto', contamination=0.1, max_features=1.0)
    X_train = df_var_caract.values
    outliers_pred = clf.fit(X_train).predict(X_train)
    index_vols_typiques = df_var_caract[outliers_pred == 1].index

    df_vols_typiques = pd.DataFrame()
    for flight in index_vols_typiques:
        df_vols_typiques = pd.concat([df_vols_typiques, df_flights.ix[flight]])

    print("nombre de vols dans notre catégorie 'vols typiques' : {}".format(len(index_vols_typiques)))

    return df_vols_typiques.reset_index()
