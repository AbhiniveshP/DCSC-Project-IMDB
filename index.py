from s3_module.s3 import S3
from sqs_module.sqs import SQS
import os, json

def main():
    s3 = S3()
    sqs = SQS()
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

if __name__ == '__main__':
    main()
