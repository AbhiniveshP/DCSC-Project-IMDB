import pandas as pd
import json, csv
import os
from s3_module.s3 import S3
from dynamodb_module.dynamodb import DynamoDB
from elasticsearch_module.es import ElasticSearch

# s3 = S3()
# ddb = DynamoDB()
es = ElasticSearch()

def csv_to_json(csv_path, json_folder_path, id_name):

    count = 0
    for rows in pd.read_csv(csv_path, sep= '\t', chunksize= 24000):

        table_name_list = os.path.basename(csv_path).split('.')[:-1]
        table_name = '_'.join(table_name_list)
        local_count = 0

        for index, row in rows.itertuples():
            local_count += 1
            current_dict = {}
            current_dict['table_name'] = table_name
            file_name = table_name + '_' + row[id_name] + '.json'
            file_path = os.path.join(json_folder_path, file_name)

            for col in rows.columns:
                current_dict[col] = row[col]
            print(current_dict)
            return
            es.update_titles_doc(table_name, current_dict, current_dict[id_name])
            # ddb.put_item(table_name, current_dict)
            # with open(file_path, 'w') as fp:
            #     json.dump(current_dict, fp)
            #
            # s3.upload_file(file_path)
            # os.remove(file_path)
            if (local_count % 100 == 0):
                print(local_count, ' done')

        count += 1
        print('===')
        print(count * 24000, ' COMPLETED...')
        print('===')

def csv_to_json_full(csv_path, json_file_path, id_name):

    data = {}

    table_name_list = os.path.basename(csv_path).split('.')[:-1]
    table_name = '_'.join(table_name_list)

    with open(csv_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter= '\t')
        count = 0

        for rows in csv_reader:

            data[id_name] = rows
            # ddb.put_item(table_name, rows)
            es.update_titles_doc(table_name, rows, rows[id_name])
            count += 1
            if (count % 50 == 0):
                print(count, ' ...Completed')

    # with open(json_file_path, 'w') as jsonfile:
    #     jsonfile.write(json.dumps(data, indent=4))

    return

csv_folder_path = os.path.join('../data', 'modified-tsvs')
json_folder_path = os.path.join('../data', 'json_files')

csv_path = os.path.join(csv_folder_path, 'title.akas.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.basics.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.crew.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.episode.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.principals.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.ratings.tsv')
# csv_path = os.path.join(csv_folder_path, 'name.basics.tsv')

# csv_to_json(csv_path, json_folder_path, 'titleId')
csv_to_json_full(csv_path, '', 'titleId')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'nconst')

