import json
import boto3
import os
import re
from botocore.exceptions import ClientError

def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    if event.get('httpMethod') != 'GET':
        return {
            'statusCode': 405,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Method Not Allowed. Only GET method is accepted.'
            })
        }
    
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        
        table_name = os.environ['STORAGE_USERS_NAME']
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
        table = dynamodb.Table(table_name)
        
        if 'id' in query_params:
            user_id = query_params['id']
            response = table.get_item(
                Key={'id': user_id}
            )
            
            if 'Item' not in response:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'message': f'User with ID {user_id} not found'
                    })
                }
            
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'user': response['Item']
                })
            }
        
        elif 'email' in query_params:
            email = query_params['email']
            
            if not is_valid_email(email):
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'message': 'Invalid email format'
                    })
                }
            
            try:
                response = table.query(
                    IndexName='emailIndex',
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
                )
                
            except ClientError as e:
                if 'ValidationException' in str(e):
                    response = table.scan(
                        FilterExpression=boto3.dynamodb.conditions.Attr('email').eq(email)
                    )
                else:
                    raise
            
            if not response.get('Items', []):
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'message': f'User with email {email} not found'
                    })
                }
            
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'user': response['Items'][0]
                })
            }
        
        else:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'message': 'Missing required parameter: either "id" or "email" must be provided'
                })
            }
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }