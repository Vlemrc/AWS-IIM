import json
import boto3
import os
import decimal
from datetime import datetime, timezone

# Clients AWS
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
s3 = boto3.client('s3', region_name="eu-west-1")

# Récupération des noms de ressources
table_name = os.environ.get('STORAGE_CRYPTOPRICES_NAME', 'CryptoPrices')
bucket_name = "aws20257d19c6899975456cafa203850e7da6ca582fa-dev"

# Conversion des Decimals en float
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

def handler(event, context):
    try:
        print("Event reçu :", json.dumps(event))

        # ✅ Vérifie que c'est un appel GET
        if event.get("httpMethod") != "GET":
            return {
                "statusCode": 405,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Méthode non autorisée. Utilisez GET."})
            }

        table = dynamodb.Table(table_name)

        # Scan de la table
        response = table.scan()
        items = response.get("Items", [])

        # Tri optionnel par email
        items.sort(key=lambda x: x.get("email", "").lower())

        # Génération d’un nom de fichier horodaté
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        filename = f"exports/crypto_{timestamp}.json"

        # Conversion en JSON
        json_data = json.dumps(items, cls=DecimalEncoder, indent=2)

        # Upload vers S3
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json_data,
            ContentType="application/json"
        )

        # Génération d'une URL pré-signée
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': filename},
            ExpiresIn=3600  # 1 heure
        )

        # ✅ Choix du format de sortie (JSON ou HTML)
        accept_header = event.get("headers", {}).get("accept", "").lower()
        if "text/html" in accept_header:
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "text/html",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": f"<html><body><p>Export réussi !</p><p><a href='{presigned_url}'>Télécharger le fichier</a></p></body></html>"
            }
        else:
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({
                    "message": "Export effectué avec succès.",
                    "download_url": presigned_url
                })
            }

    except Exception as e:
        print("Erreur :", str(e))
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": "Erreur lors de l'export des données."})
        }
