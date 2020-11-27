import boto3
import os
from aws_credentials import *


class S3:

    def __init__(self, bucket_name='dcsc2020-imdb'):

        self.s3_client = boto3.client('s3', region_name=region_name,
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key,
                                      aws_session_token=aws_session_token)

    def create_bucket(self, bucket_name='dcsc2020-imdb'):
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

    def get_all_objects(self, bucket_name='dcsc2020-imdb'):

        result = []

        try:
            for resp in self.s3_client.list_objects(Bucket=bucket_name)['Contents']:
                key_name = resp['Key']
                result.append(self.get_object(key_name))

        except:
            pass

        return result

    def get_object(self, key_name, bucket_name='dcsc2020-imdb'):

        try:
            return self.s3_client.get_object(Bucket=bucket_name, Key=key_name)

        except:
            return None

    def upload_file(self, file_path, bucket_name='dcsc2020-imdb'):

        try:
            self.s3_client.upload_file(Filename=file_path,
                                       Bucket=bucket_name,
                                       Key=os.path.basename(file_path))

        except:
            pass

        return

    def delete_object(self, key_name, bucket_name='dcsc2020-imdb'):

        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=key_name)

        except:
            pass

        return

    def upload_file_public(self, file_path, bucket_name='dcsc2020-imdb'):

        try:
            self.s3_client.upload_file(Filename=file_path,
                                       Bucket=bucket_name,
                                       Key=os.path.basename(file_path),
                                       ExtraArgs= {'ACL': 'public-read'})

        except:
            pass

        return

    def upload_html_public(self, file_path, bucket_name='dcsc2020-imdb'):

        try:
            self.s3_client.upload_file(Filename=file_path,
                                       Bucket=bucket_name,
                                       Key=os.path.basename(file_path),
                                       ExtraArgs= {
                                           'ContentType': 'text/html',
                                           'ACL': 'public-read'})

        except:
            pass

        return