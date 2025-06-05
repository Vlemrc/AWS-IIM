import json
import boto3
import os 
import uuid
import re

dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
table_name = os.environ['STORAGE_USERS_NAME']
table = dynamodb.Table(table_name)

EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"

def handler(event, context):
    print("Event received:", event)

    # Vérification de la méthode HTTP
    if event.get("httpMethod") != "POST":
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Méthode non autorisée. Utilisez POST."})
        }

    try:
        body = json.loads(event.get("body", "{}"))
        name = body.get("name")
        email = body.get("email")

        # Contrôle des champs obligatoires
        if not name or not email:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Les champs 'name' et 'email' sont obligatoires."})
            }

        # Vérification du format de l'email
        if not re.match(EMAIL_REGEX, email):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Adresse email invalide."})
            }

        # Vérification si l'email existe déjà (scan ici, mais idéalement via GSI)
        response = table.scan(
            FilterExpression="email = :e",
            ExpressionAttributeValues={":e": email}
        )
        if response.get("Items"):
            return {
                "statusCode": 409,
                "body": json.dumps({"error": "L'adresse email existe déjà."})
            }

        res = table.query(
            IndexName='emails',
            KeyConditionExpression=Key('email').eq(body['email'])
        )

        # Création de l'utilisateur
        table.put_item(
            Item={
                "id": str(uuid.uuid4()),
                "name": name,
                "email": email,
            }
        )

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Utilisateur créé avec succès."})
        }

    except Exception as e:
        print("Erreur:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Une erreur est survenue lors de la création de l'utilisateur."})
        }
