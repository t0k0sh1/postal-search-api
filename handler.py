import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')


def handle(event, context):
    keyword = event['queryStringParameters']['q']

    try:
        table = dynamodb.Table('PostalCode')

        res = table.scan(
            FilterExpression=Key('ZipCode').begins_with(keyword) | Key('PrefName').begins_with(
                keyword) | Key('CityName').begins_with(keyword) | Key('TownName').begins_with(keyword)
        )

        body = []
        for item in res['Items']:
            body.append({
                "zipcode": item['ZipCode'],
                "pref_name": item['PrefName'],
                "pref_name_kana": item['PrefNameKana'],
                "city_name": item['CityName'],
                "city_name_kana": item['CityNameKana'],
                "town_name": item['TownName'],
                "town_name_kana": item['TownNameKana']
            })

        response = {
            "statusCode": 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "X-Requested-With, Origin, X-Csrftoken, Content-Type, Accept"
            },
            "body": json.dumps(body)
        }
        return response

    except ClientError as e:
        response = {
            "statusCode": 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "X-Requested-With, Origin, X-Csrftoken, Content-Type, Accept"
            },
            "body": json.dumps({"message": e.response['Error']['Message']})
        }
        return response
