# -*- coding: utf-8 -*-

"""
Remplit le fichier de métriques de supervision sur les différents indicateurs.
Pour chaque indicateur, chaque jour, on ajoute les métriques suivantes :
    - nombre de prédictions
    - moyenne
    - min
    - q25
    - q50
    - q75
    - max
    - std
    - %_1 (pourcentage class 1)
    - %_2
    - %_3
    - date
"""
import argparse
import os
import pandas as pd
from datetime import date

from pourvoi_et_candidatures.model.utils.bucket_functions import send_file

def main():
    # Create the parser and get the Name of the prediction
    my_parser = argparse.ArgumentParser(description='Name of the prediction')
    my_parser.add_argument('Name')
    args = my_parser.parse_args()
    pred_name = args.Name

    day = date.today().strftime("%Y-%m-%d")
    metrics_filename = pred_name + '_metrics.csv'
    metrics_filename_day = pred_name + '_metrics_' + day + '.csv'

 
    pred_filename = './pourvoi_et_candidatures/model/data/' + pred_name + '.csv'
    pred_file = pd.read_csv(pred_filename, sep='|')

    # Calculate the count, mean, std, q25, q50, q75, max
    distribution_metrics = pd.DataFrame(pred_file.describe()["pred"]).T.reset_index(drop=True)

    # Calculate the class distribution
    class_percents = (pred_file.pred_attract_class.value_counts() /
                      len(pred_file)* 100).sort_index()
    class_percents = pd.DataFrame(class_percents).T.reset_index(drop=True)
    class_percents.columns = ['%_1', '%_2', '%_3']
    
    # daily metrics to add
    daily_metrics = distribution_metrics.join(class_percents).round().astype(int)
    daily_metrics['day'] = day

    if not os.path.exists("metrics"):
        os.mkdir("metrics")

    ## 1. save metrics in common file
    # Case 1 : the metrics file exists
    try:
        metrics_file = pd.read_csv(os.path.join("metrics",metrics_filename), sep='|')
        metrics_file = metrics_file.append(daily_metrics)
    # Case 2 : the metrics file has not yet been created
    except:
        metrics_file = daily_metrics

    metrics_file.to_csv(os.path.join("metrics",metrics_filename), index=False, sep='|')
    # send file to bucket
    send_file(os.path.join("metrics",metrics_filename), 
              'supervision-metrics/' + metrics_filename)

if __name__ == '__main__':
    main()
    