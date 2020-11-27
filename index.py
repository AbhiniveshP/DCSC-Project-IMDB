from s3_module.s3 import S3
from sqs_module.sqs import SQS
from dynamodb_module.dynamodb import DynamoDB
import os, json

def main():
    s3 = S3()
    sqs = SQS()
    ddb = DynamoDB()
    file_path = os.path.join('data', 'json_files', 'name_basics_nm0000001.json')
    json_folder = os.path.join('data', 'json_files')
    fobj = open(file_path)

    # s3.upload_file(file_path)

    for file in os.listdir(json_folder):
        file_path = os.path.join(json_folder, file)
        s3.upload_file(file_path)

    # sqs.send_message('title_akas', message= data)
    # mb, rh = sqs.receive_message('title_akas')
    # print(mb)
    # sqs.delete_message('title_akas', rh)

    # ddb.get_table_info('tt0000001')
    # ddb.get_name_info('nm0000003')

if __name__ == '__main__':
    main()
