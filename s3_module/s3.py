import boto3
import os
from aws_credentials import *

class S3:

    def __init__(self, bucket_name='dcsc2020-imdb'):

        self.session = boto3.Session(profile_name='default')
        self.s3_client = self.session.client('s3')
        self.bucket = self.session.resource('s3').Bucket(bucket_name)
        
    def create_bucket(self, bucket_name):

        self.s3_client.create_bucket(Bucket=bucket_name)

    def delete_all_objects(self, bucket_name='dcsc2020-imdb'):

        try:
            for resp in self.s3_client.list_objects(Bucket=bucket_name)['Contents']:
                key_name = resp['Key']
                self.s3_client.delete_object(Bucket=bucket_name, Key=key_name)

        except:
            pass

        return

    def list_all_objects(self, bucket_name='dcsc2020-imdb'):

        keys = set([resp['Key'] for resp in self.s3_client.list_objects(Bucket=bucket_name)['Contents']])

        return keys

    def upload_all_files(self, directory_path, bucket_name='dcsc2020-imdb'):

        for file_name in os.listdir(directory_path):

            if file_name.endswith('.tsv'):

                file_path = os.path.join(directory_path, file_name)
                self.s3_client.upload_file(Filename=file_path,
                               Bucket=bucket_name,
                               Key=file_name)

        return