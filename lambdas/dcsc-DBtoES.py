import json, requests
from elasticsearch import Elasticsearch
from elasticsearch_module.es import ElasticSearch


# import requests

def lambda_handler(event, context):
    # TODO implement

    es = ElasticSearch()

    for record in event['Records']:
        action = record['eventName']
        table_name = record['eventSourceARN'].split('/')[1]
        print(table_name, record)
        table_json = {}

        if (action == 'INSERT' or action == 'MODIFY'):

            table_json = record['dynamodb']['NewImage']
            for key, value_dict in table_json.items():
                for data_type, final_value in value_dict.items():

                    if data_type == 'N':

                        try:
                            table_json[key] = int(final_value)
                        except:
                            table_json[key] = float(final_value)

                    elif data_type == 'S':
                        table_json[key] = str(final_value)

        if ('title' in table_name):
            if ('akas' in table_name):
                title_id = table_json['titleId']
            else:
                title_id = table_json['tconst']
            es.update_titles_doc(table_name, table_json, title_id)
            print(table_name, table_json, title_id)

        elif ('name' in table_name):
            person_id = table_json['nconst']
            es.update_people_doc(table_json, person_id)
            print(table_json, person_id)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
