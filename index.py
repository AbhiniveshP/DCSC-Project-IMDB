from s3_module.s3 import S3
import os

def main():
    s3 = S3()
    file_path = os.path.join('data', 'json_files', 'title_akas_tt0000001.json')
    print(file_path)
    s3.upload_file(file_path)

if __name__ == '__main__':
    main()
