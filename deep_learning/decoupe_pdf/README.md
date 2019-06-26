# decoupe-doc-ged
<br/>
Pour lancer l'entrainement d'un nouveau modèle :
<br/>
<br/>Créer, dans le dossier experiments, un nouveau dossier base_model avec le fichier params.
<br/>Mettre à jour, dans le fichier train.py, la variable 'model_name'.
<br/>
<br/>Lancer l'entrainement : python train.py (les paramètres de l'entrainement sont définis dans le fichier
params)
<br/>
<br/>Une fois l'entrainement terminé, lancer l'évaluation sur le test set : python evaluate.py

