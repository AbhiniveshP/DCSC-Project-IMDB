import json, csv, codecs
from s3_module.s3 import S3
from sqs_module.sqs import SQS
from dynamodb_module.dynamodb import DynamoDB

def lambda_handler(event, context):
    # TODO implement
    
    s3 = S3()
    sqs = SQS()
    
    key_name = event['Records'][0]['s3']['object']['key']
    
    data = s3.get_object(key_name)['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    
    queue_name = json_data['table_name']
    sqs.send_message(queue_name, json_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
