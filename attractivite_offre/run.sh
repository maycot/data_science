#!/bin/bash
git pull

echo "Vues et candidature model"
cd ~/attractivite_offre/pourvoi_et_candidatures
if [ -d "$DIR" ]; then
    echo "Activating environment..."
    source env/bin/activate
    python -m pip install -q --upgrade pip
    pip install -q -r requirements.txt
else
    echo "Creating environment..."
    python -m venv env
    source env/bin/activate
    python -m pip install --upgrade pip
    pip install -r requirements.txt
fi
cd model
python main.py

# echo "metriques de supervision"
cd ~/attractivite_offre/
python supervision_metrics.py candidature
python supervision_metrics.py pourvoi
