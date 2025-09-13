# -*- coding: utf-8 -*-

import boto3

BUCKET_NAME = 'x-da062-save-s3'
AWS_ACCESS_KEY_ID = 'x-da062-save-s3'
AWS_SECRET_ACCESS_KEY = "uTFHe5ZimVxmfkwxH389WToNbjDI0WFvwVG3wVXe"
ENDPOINT_URL = "http://stockage-ecs1-qvr.sii24.pole-emploi.intra:9020"

CLIENT = boto3.client('s3', endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY, use_ssl=False)

def send_file(object_name: str, filename: str):
    """Envoi un fichier du poste vers le bucket
    ---
    Args:
        object_name: (string) chemin du fichier à envoyer
        filename: (string) nom du fichier dans le bucket
    """
    try:
        CLIENT.upload_file(object_name, BUCKET_NAME, filename)
    except:
        print("Error occured")

def get_file(object_name: str, filename: str):
    """Télécharge un fichier du bucket vers le poste
    ---
    Args:
        object_name: (string) chemin du fichier
        filename: (string) nom du fichier dans le bucket
    """
    try:
        CLIENT.download_file(BUCKET_NAME, filename, object_name)
    except:
        print("Error occured")
