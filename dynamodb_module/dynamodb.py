import boto3
import os
import json
from aws_credentials import *

class DynamoDB:

    def __init__(self):
        self.ddb_client = boto3.client('dynamodb', region_name=region_name,
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key,
                                      aws_session_token=aws_session_token)
        self.existing_tables =  self.ddb_client.list_tables()['TableNames']

    def check_if_table_exists(self, table_name):
        return table_name in self.existing_tables

    def put_item(self, table_name, data):
        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            table = self.ddb_client.Table(table_name)

            response = table.put_item(Item=data)
            return response

        except Exception as e:
            print(e)
            raise

    def get_item(self, table_name, key):
        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            table = self.ddb_client.Table(table_name)

            try:
                response = table.get_item(Key=key)
            except Exception as e:
                print(e)
                raise
            else:
                return response['Item']
        except Exception as e:
            print(e)
            raise

    def delete_item(self, table_name, key):
        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            table = self.ddb_client.Table(table_name)

            try:
                response = table.delete_item(Key=key)
            except Exception as e:
                print(e)
            else:
                return response
        except Exception as e:
            print(e)
            raise
