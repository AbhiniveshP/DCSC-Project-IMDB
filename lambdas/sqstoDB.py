import json, csv, codecs
from sqs_module.sqs import SQS
from dynamodb_module.dynamodb import DynamoDB
from decimal import Decimal

def lambda_handler(event, context):
    # TODO implement
    
    sqs = SQS()
    dynamodb = DynamoDB()
    
    queue_name = event['Records'][0]['eventSourceARN'].split(":")[-1]
    
    print("queue_name: {}".format(queue_name))
    
    print("event: {}".format(event))
    
    # message, receipt_handle = sqs.receive_message(queue_name)
    
    message = event['Records'][0]['body']
    receipt_handle = event['Records'][0]['receiptHandle']
    print("mb:", message)
    
    sqs.delete_message(queue_name, receipt_handle)
    
    data = json.loads(message, parse_float=Decimal)
    
    dynamodb.put_item(table_name=queue_name, data=data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
