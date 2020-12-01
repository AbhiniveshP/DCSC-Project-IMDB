from s3_module.s3 import S3
from sqs_module.sqs import SQS
from dynamodb_module.dynamodb import DynamoDB
from elasticsearch_module.es import ElasticSearch
import os, json

def main():
    s3 = S3()
    # sqs = SQS()
    # ddb = DynamoDB()
    es = ElasticSearch()
    file_path = os.path.join('data', 'json_files', 'title_akas_tt0000258.json')
    json_folder = os.path.join('data', 'json_files')
    fobj = open(file_path)

    s3.upload_file(file_path)

    # for file in os.listdir(json_folder):
    #     file_path = os.path.join(json_folder, file)
    #     # s3.upload_file(file_path)
    #
    #     if ('title' in file_path):
    #         with open(file_path) as json_file:
    #             print(json_file)
    #             table_json = json.load(json_file)
    #             table_name = table_json['table_name']
    #             title_id = None
    #             if ('akas' in file_path):
    #                 title_id = table_json['titleId']
    #             else:
    #                 title_id = table_json['tconst']
    #             es.update_titles_doc(table_name, table_json, title_id)
    #             print(es.get_json_from_es('titles', title_id))
    #
    #     if ('name' in file_path):
    #         with open(file_path) as json_file:
    #             print(json_file)
    #             table_json = json.load(json_file)
    #             person_id = table_json['nconst']
    #             es.update_people_doc(table_json, person_id)
    #             print(es.get_json_from_es('people', person_id))





    # sqs.send_message('title_akas', message= data)
    # mb, rh = sqs.receive_message('title_akas')
    # print(mb)
    # sqs.delete_message('title_akas', rh)

    # ddb.get_table_info('tt0000001')
    # ddb.get_name_info('nm0000003')

if __name__ == '__main__':
    main()
