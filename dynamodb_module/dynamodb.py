import boto3
import os
import json
import pandas as pd
from aws_credentials import *
from dynamodb_module.reserved import reserved_keywords
import uuid

class DynamoDB:

    def __init__(self):
        self.ddb_client = boto3.client('dynamodb', region_name=region_name,
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key,
                                       aws_session_token=aws_session_token)

        self.ddb_resource = boto3.resource('dynamodb', region_name=region_name,
                                           aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key,
                                           aws_session_token=aws_session_token)

        self.primary_key = {'name_basics': 'nconst',
                            'title_akas': 'titleId',
                            'title_basics': 'tconst',
                            'title_crew': 'tconst',
                            'title_episode': 'tconst',
                            'title_principals': 'tconst',
                            'title_ratings': 'tconst'}

        # self.existing_tables = self.ddb_client.list_tables()['TableNames']

    def check_if_table_exists(self, table_name):
        return table_name in self.primary_key

    def put_item(self, table_name, data):
        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            if self.primary_key[table_name] not in data:
                raise Exception("Payload is missing primary key: {}\n{}".format(self.primary_key[table_name], data))

            table = self.ddb_resource.Table(table_name)
            data.pop('table_name')
            response = table.put_item(Item=data)
            return response

        except Exception as e:
            print(e)
            raise

    def update_item(self, table_name, data):
        def get_update_params(body):
            def get_random_string():
                new_key = "#{}".format(str(uuid.uuid4()))
                new_key = "".join([c for c in new_key if not c.isdigit() and c != '-'])
                return new_key

            update_expression = ["set "]
            update_values = dict()
            update_names = dict()

            for key, val in body.items():
                if key in reserved_keywords:
                    new_key = get_random_string()
                    update_names[new_key] = key
                    update_expression.append(f"{new_key} = :{new_key[1:]},")
                    update_values[f":{new_key[1:]}"] = val
                else:
                    update_expression.append(f"{key} = :{key},")
                    update_values[f":{key}"] = val

            return "".join(update_expression)[:-1], update_values, update_names

        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            if self.primary_key[table_name] not in data:
                raise Exception("Payload is missing primary key: {}\n{}".format(self.primary_key[table_name], data))

            key = data[self.primary_key[table_name]]

            table = self.ddb_resource.Table(table_name)
            data.pop('table_name')
            data.pop(self.primary_key[table_name])
            update_expr, expr_attrs, expr_names = get_update_params(data)
            if expr_names:
                response = table.update_item(
                    Key={self.primary_key[table_name]: key}, UpdateExpression=update_expr, 
                    ExpressionAttributeValues=expr_attrs, ExpressionAttributeNames=expr_names, ReturnValues="UPDATED_NEW"
                )
            else:
                response = table.update_item(
                    Key={self.primary_key[table_name]: key}, UpdateExpression=update_expr, 
                    ExpressionAttributeValues=expr_attrs, ReturnValues="UPDATED_NEW"
                )
            return response
        except Exception as e:
            print(e)
            raise

    def get_item(self, table_name, key):
        try:
            response = self.ddb_client.get_item(TableName=table_name, Key=key)
            return response['Item']

        except Exception as e:
            print(e)
            raise

    def delete_item(self, table_name, key):
        try:
            if not self.check_if_table_exists(table_name):
                raise Exception("Table {} does not exist".format(table_name))

            if self.primary_key[table_name] not in key:
                raise Exception("Key is missing primary key: {}\n{}".format(self.primary_key[table_name], key))

            try:
                table = self.ddb_resource.Table(table_name)
                response = table.delete_item(Key=key)
            except Exception as e:
                print(e)
            else:
                return response
        except Exception as e:
            print(e)
            raise

    def get_table_info(self, title_id):

        key1 = {'titleId': {'S': title_id}}
        key2 = {'tconst': {'S': title_id}}

        title_akas = self.get_item('title_akas', key1)
        title_basics = self.get_item('title_basics', key2)
        title_crew = self.get_item('title_crew', key2)
        title_ratings = self.get_item('title_ratings', key2)

        title_info = {}

        if title_akas != {}:
            title_info['title'] = title_akas['title']['S']
            title_info['attributes'] = title_akas['attributes']['S']
            title_info['isOriginalTitle'] = int(title_akas['isOriginalTitle']['N']) if 'N' in title_akas[
                'isOriginalTitle'] else 0
            title_info['language'] = title_akas['language']['S']
            title_info['region'] = title_akas['region']['S']
            title_info['types'] = title_akas['types']['S']
        if (title_basics != {}):
            title_info['endYear'] = int(title_basics['endYear']['N']) if 'N' in title_basics['endYear'] else int(
                title_basics['startYear']['N'])
            title_info['startYear'] = int(title_basics['startYear']['N'])
            title_info['genres'] = title_basics['genres']['S']
            title_info['isAdult'] = int(title_basics['isAdult']['N'])
            title_info['originalTitle'] = title_basics['originalTitle']['S']
            title_info['primaryTitle'] = title_basics['primaryTitle']['S']
            title_info['runtimeMinutes'] = int(title_basics['runtimeMinutes']['S'])
        if (title_ratings != {}):
            title_info['averageRating'] = float(title_ratings['averageRating']['N'])
            title_info['numVotes'] = int(title_ratings['numVotes']['N'])

        if (title_crew != {}):

            title_info['directors'] = []
            for director in title_crew['directors']['S'].split(','):
                nkey = {'nconst': {'S': director}}
                name_basics = self.get_item('name_basics', nkey)
                if (name_basics != {}):
                    title_info['directors'].append(name_basics['primaryName']['S'])

            title_info['writers'] = []
            for writer in title_crew['writers']['S'].split(','):
                nkey = {'nconst': {'S': writer}}
                name_basics = self.get_item('name_basics', nkey)
                if (name_basics != {}):
                    title_info['writers'].append(name_basics['primaryName']['S'])

            title_info['directors'] = ''.join(title_info['directors'])
            title_info['writers'] = ''.join(title_info['writers'])

        df = pd.DataFrame(title_info.items(), columns=['Information Type', 'Value'])
        file_name = '/tmp/title_info_' + title_id + '.html'
        print(df.to_html(file_name))
        return df

    def get_name_info(self, name_id):

        key = {'nconst': {'S': name_id}}

        name_basics = self.get_item('name_basics', key)

        name_info = {}

        if (name_basics != {}):
            name_info['primaryName'] = name_basics['primaryName']['S']
            name_info['birthYear'] = name_basics['birthYear']['S']
            name_info['deathYear'] = name_basics['deathYear']['S']
            name_info['primaryProfession'] = name_basics['primaryProfession']['S']

            name_info['titlesActed'] = []
            for title in name_basics['knownForTitles']['S'].split(','):
                tkey = {'titleId': {'S': title}}
                title_akas = self.get_item('title_akas', tkey)
                if (title_akas != {}):
                    name_info['titlesActed'].append(title_akas['title']['S'])

            name_info['titlesActed'] = ''.join(name_info['titlesActed'])

        df = pd.DataFrame(name_info.items(), columns=['Information Type', 'Value'])
        file_name = '/tmp/name_info_' + name_id + '.html'
        print(df.to_html(file_name))
        return df