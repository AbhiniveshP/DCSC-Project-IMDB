import boto3
import os
from aws_credentials import *

s3 = boto3.client('s3', region_name= region_name,
                  aws_access_key_id = aws_access_key_id,
                  aws_secret_access_key = aws_secret_access_key,
                  aws_session_token = aws_session_token)

s3.create_bucket(Bucket = 'dcsc2020-imdb')

keys = set( [ resp['Key'] for resp in s3.list_objects(Bucket = 'dcsc2020-imdb')['Contents'] ] )

for file_name in os.listdir('../data'):

    if file_name.endswith('.tsv') and file_name not in keys:

        file_path = os.path.join('../data', file_name)
        s3.upload_file(Filename = file_path,
                       Bucket = 'dcsc2020-imdb',
                       Key = file_name)
        print(file_name, ' ...done!')
