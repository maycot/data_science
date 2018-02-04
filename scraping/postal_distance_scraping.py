# -*- coding: utf-8 -*-
"""
Quick scraping work to scrap distance in km between 2 postal code on website:
    www.le-codepostal.com
"""

import numpy as np
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import json

df = pd.read_csv("pos_offres.csv", sep = ",", index_col=0, dtype="unicode")
df["voiture_distance"] = 0
df["oiseau_distance"] = 0


for i in df.index:

    if i % 1000 == 0:
        print("current index : {}".format(i))

    query = ""
    de_commune = df.loc[i, "de_commune"].strip().replace(" ", "-")
    offre_commune = df.loc[i, "offre_commune"].strip().replace(" ","-")
    query = de_commune + "/" + offre_commune

    url = "http://www.le-codepostal.com/itineraire/" + query
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "html.parser")

    if soup.find("table", attrs={"class": "resume"}):
        #print("found")
        resume = soup.find("table", attrs={"class": "resume"})
        details_list = resume.get_text().split("\n")

        for j in range(len(details_list)):
            if "Distance en voiture" in details_list[j]:
                df.loc[i, "voiture_distance"] = details_list[j+1]
            elif "Distance à vol d'oiseau" in details_list[j]:
                df.loc[i, "oiseau_distance"] = details_list[j+1]
    else:
        #print("not found")
        df.loc[i, "voiture_distance"] = "no"
        df.loc[i, "oiseau_distance"] = "no"
