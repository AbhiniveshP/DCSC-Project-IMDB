import pandas as pd
import json
import os

def csv_to_json(csv_path, json_folder_path, id_name):

    print('Started...')
    rows = pd.read_csv(csv_path, sep= '\t')
    table_name_list = os.path.basename(csv_path).split('.')[:-1]
    table_name = '_'.join(table_name_list)

    count = 0
    for index, row in rows.iterrows():

        current_dict = {}
        current_dict['table_name'] = table_name

        for col in rows.columns:
            file_name = table_name + '_' + row[id_name] + '.json'
            file_path = os.path.join(json_folder_path, file_name)
            current_dict[col] = row[col]
            with open(file_path, 'w') as fp:
                json.dump(current_dict, fp)

        count += 1
        if ( (count)  % 5 == 0):
            return

    print(table_name, ' completed...')

csv_folder_path = os.path.join('../data', 'latest-tsvs')
json_folder_path = os.path.join('../data', 'json_files')

# csv_path = os.path.join(csv_folder_path, 'title.akas.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.basics.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.crew.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.episode.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.principals.tsv')
# csv_path = os.path.join(csv_folder_path, 'title.ratings.tsv')
csv_path = os.path.join(csv_folder_path, 'name.basics.tsv')

# csv_to_json(csv_path, json_folder_path, 'titleId')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
# csv_to_json(csv_path, json_folder_path, 'tconst')
csv_to_json(csv_path, json_folder_path, 'nconst')

