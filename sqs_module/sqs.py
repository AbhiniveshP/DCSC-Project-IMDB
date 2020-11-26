import boto3
import os
import json
from aws_credentials import *

class SQS:

    def __init__(self):
        self.sqs_client = boto3.client('sqs', region_name=region_name,
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key,
                                      aws_session_token=aws_session_token)

    def send_message(self, queue_name, message= 'Hello World' ):

        data = json.dumps(message)
        queue_url = self.sqs_client.get_queue_url(QueueName= queue_name)['QueueUrl']
        response = self.sqs_client.send_message(QueueUrl= queue_url, MessageBody= data)
        return response

    def receive_message(self, queue_name):

        queue_url = self.sqs_client.get_queue_url(QueueName= queue_name)['QueueUrl']
        response = self.sqs_client.receive_message(QueueUrl= queue_url)
        message = response['Messages'][0]
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']

        return message_body, receipt_handle

    def delete_message(self, queue_name, receipt_handle):

        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        response = self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle= receipt_handle)
        return



