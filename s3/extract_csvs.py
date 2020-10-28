import os
import gzip
import shutil

for file_name in os.listdir('../data'):

    if file_name.endswith('.gz'):

        file_path = os.path.join('../data', file_name)
        extracted_file_path = os.path.join('../data', file_name[:-3])

        with gzip.open(file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        os.remove(file_path)

