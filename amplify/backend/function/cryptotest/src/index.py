import json
import urllib.request
import urllib.error
import boto3
from decimal import Decimal
import datetime
import os

# DynamoDB Configuration
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
table_name = os.environ.get('STORAGE_CRYPTOPRICES_NAME', 'CryptoPrices')
table = dynamodb.Table(table_name)

def clear_old_crypto_data():
    """
    Clears all existing crypto data to replace with fresh data
    """
    try:
        # Scan all items and delete them
        response = table.scan()
        with table.batch_writer() as batch:
            for item in response['Items']:
                batch.delete_item(
                    Key={
                        'crypto_id': item['crypto_id'],
                        'timestamp': item['timestamp']
                    }
                )
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            with table.batch_writer() as batch:
                for item in response['Items']:
                    batch.delete_item(
                        Key={
                            'crypto_id': item['crypto_id'],
                            'timestamp': item['timestamp']
                        }
                    )
        
        return {'success': True}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def save_crypto_to_db(data):
    """
    Replaces all crypto data in DynamoDB by first clearing old data then inserting new
    """
    try:
        # First, clear existing data
        clear_result = clear_old_crypto_data()
        if not clear_result['success']:
            return {
                'success': False,
                'error': f'Failed to clear old data: {clear_result["error"]}'
            }
        
        current_timestamp = datetime.datetime.utcnow().isoformat()
        
        # Then insert new data
        with table.batch_writer() as batch:
            for coin in data:
                batch.put_item(
                    Item={
                        "crypto_id": coin["id"],
                        "timestamp": current_timestamp,
                        "name": coin["name"],
                        "symbol": coin["symbol"],
                        "price": Decimal(str(coin["current_price"])),
                        "market_cap": Decimal(str(coin["market_cap"])) if coin["market_cap"] else Decimal('0'),
                        "market_cap_rank": coin["market_cap_rank"],
                        "price_change_24h": Decimal(str(coin["price_change_percentage_24h"])) if coin["price_change_percentage_24h"] else Decimal('0')
                    }
                )
        
        return {
            'success': True,
            'message': f'Replaced all data with {len(data)} fresh cryptocurrencies in DynamoDB'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Error saving to DynamoDB: {str(e)}'
        }


def get_top_crypto_prices():
    """
    Récupère les prix des 10 plus grosses cryptomonnaies depuis CoinGecko API
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
        
        # Return both raw data for DB and formatted data for API response
        crypto_prices = []
        for crypto in data:
            crypto_info = {
                'name': crypto['name'],
                'symbol': crypto['symbol'].upper(),
                'current_price': crypto['current_price'],
                'market_cap': crypto['market_cap'],
                'market_cap_rank': crypto['market_cap_rank'],
                'price_change_percentage_24h': crypto['price_change_percentage_24h']
            }
            crypto_prices.append(crypto_info)
        
        return {
            'success': True,
            'data': crypto_prices,
            'raw_data': data  # Include raw data for DB saving
        }
    except urllib.error.URLError as e:
        return {
            'success': False,
            'error': f'Erreur de connexion: {str(e)}'
        }
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Erreur de décodage JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Erreur inattendue: {str(e)}'
        }

def handler(event, context):
    print('received event:')
    print(event)
    
    # Get cryptocurrency data from API
    crypto_data = get_top_crypto_prices()
    
    if crypto_data['success']:
        # Save data to DynamoDB
        db_result = save_crypto_to_db(crypto_data['raw_data'])
        
        if db_result['success']:
            response_body = {
                'message': 'Prix des 10 plus grosses cryptomonnaies récupérés et sauvegardés avec succès',
                'crypto_prices': crypto_data['data'],
                'db_operation': db_result['message'],
                'timestamp': datetime.datetime.utcnow().isoformat()
            }
            status_code = 200
        else:
            # API call succeeded but DB save failed
            response_body = {
                'message': 'Prix récupérés mais erreur lors de la sauvegarde en base',
                'crypto_prices': crypto_data['data'],
                'db_error': db_result['error'],
                'timestamp': datetime.datetime.utcnow().isoformat()
            }
            status_code = 206  # Partial Content - some operations succeeded
    else:
        response_body = {
            'message': 'Erreur lors de la récupération des prix des cryptomonnaies',
            'error': crypto_data['error'],
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        status_code = 500
    
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(response_body)
    }
