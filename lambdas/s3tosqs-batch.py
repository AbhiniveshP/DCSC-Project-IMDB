import json, csv, codecs
from s3_module.s3 import S3
from sqs_module.sqs import SQS
from decimal import Decimal

def lambda_handler(event, context):
    # TODO implement
    
    s3 = S3()
    sqs = SQS()
    
    key_name = event['Records'][0]['s3']['object']['key']
    
    print(key_name)
    data = s3.get_object(key_name)['Body'].read().decode('utf-8')
    json_data = json.loads(data, parse_float=Decimal)
    
    queue_name = json_data['table_name']
    sqs.send_message(queue_name, json_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello World')
    }
