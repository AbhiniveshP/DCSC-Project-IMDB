import json
from dynamodb_module.dynamodb import DynamoDB
from s3_module.s3 import S3
import pandas as pd
from decimal import Decimal

def get_title_id_info(title_id):
    
    ddb = DynamoDB()
    s3 = S3()
    ddb.get_table_info(title_id)
    file_name = '/tmp/title_info_' + title_id + '.html'
    s3.upload_html_public(file_name)
    return 'https://dcsc2020-imdb.s3.amazonaws.com/title_info_' + title_id + '.html'
    
def get_name_id_info(name_id):
    
    ddb = DynamoDB()
    s3 = S3()
    ddb.get_name_info(name_id)
    file_name = '/tmp/name_info_' + name_id + '.html'
    s3.upload_html_public(file_name)
    return 'https://dcsc2020-imdb.s3.amazonaws.com/name_info_' + name_id + '.html'
    
def put_in_table(table_name, payload):
    ddb = DynamoDB()
    response = ddb.put_item(table_name=table_name, data=payload)
    return response
    
def update_table(table_name, payload):
    ddb = DynamoDB()
    response = ddb.update_item(table_name=table_name, data=payload)
    return response

def delete_from_table(table_name, payload):
    ddb = DynamoDB()
    response = ddb.delete_item(table_name=table_name, key=payload)
    return response


def lambda_handler(event, context):
    public_url = None

    print(event)

    try:
        if event["httpMethod"] == 'GET':
            if ("titleId" in event['queryStringParameters']):
                title_id = event['queryStringParameters']["titleId"]
                public_url = get_title_id_info(title_id)
            elif ("nameId" in event['queryStringParameters']):
                name_id = event['queryStringParameters']["nameId"]
                public_url = get_name_id_info(name_id)
        elif event["httpMethod"] == 'POST':
            payload = json.loads(event["body"], parse_float=Decimal) 
            if 'action' in payload:
                if payload["action"] == 'PUT':
                    table_name = payload.get("table_name")
                    payload.pop('action')
                    if table_name:
                        response = put_in_table(table_name, payload)
                    else:
                        print("Invalid payload: {}".format(payload))
                elif payload["action"] == 'UPDATE':
                    table_name = payload.get("table_name")
                    payload.pop('action')
                    if table_name:
                        response = update_table(table_name, payload)
                    else:
                        print("Invalid payload: {}".format(payload))
            else:
                print("Invalid payload: {}".format(payload))
        elif event["httpMethod"] == "DELETE":
            payload = json.loads(event["body"], parse_float=Decimal) 
            if 'action' in payload:
                if payload["action"] == 'DELETE':
                    table_name = payload.get("table_name")
                    payload.pop('action')
                    payload.pop('table_name')
                    if table_name:
                        response = delete_from_table(table_name, payload)
                    else:
                        print("Invalid payload: {}".format(payload))
                else:
                    print("Invalid payload: {}".format(payload))
            else:
                print("Invalid payload: {}".format(payload))
    except Exception as e:
        print(e)
        print(event)
        raise
        
    if public_url:
        response = {
        'statusCode': 200,
        'body': json.dumps({'URL to object:': public_url})
        }
    else:
        response = {
            'statusCode': 200,
            'body': json.dumps('Success!')
        }

    return response
