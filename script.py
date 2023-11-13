from fabric import Connection
import stat
import os
import concurrent.futures
import csv
import shutil
import json
from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api

from itertools import islice
from dotenv import load_dotenv
import requests
import zipfile

load_dotenv()

def print_contents(sftp):
    files = sftp.listdir()
    print('Current directory: {}'.format(sftp.getcwd()))
    for file in files:
        print(file)

def get_latest_child_dir(sftp, path):
    dirs = [x.filename for x in sftp.listdir_attr(path) if stat.S_ISDIR(x.st_mode)]
    return sorted(dirs, reverse=True)[0]

def convert_path_to_bucket(path, prefix, suffix = None):
    name = prefix + '-' + path.replace('/', '-').replace('.', '-')
    return name if suffix is None else name + '-' + str(suffix)

def main():
    if os.path.isdir('temp/'):
        shutil.rmtree('temp/', ignore_errors=True)

    if os.path.isdir('output/'):
        shutil.rmtree('output/', ignore_errors=True)

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    application_key_id = os.getenv('B2_APPLICATION_KEY_ID')
    application_key = os.getenv('B2_APPLICATION_KEY')
    b2_api.authorize_account("production", application_key_id, application_key)

    file_path = ''

    bucket = None

    with Connection(os.getenv('CH_URL'), user=os.getenv('CH_USER'), connect_kwargs={"key_filename": os.getenv('SSH_KEY_PATH')}) as c:
        path = 'free/prod217'
        with c.sftp() as sftp:
            path += '/' + get_latest_child_dir(sftp, path)
            path += '/' + get_latest_child_dir(sftp, path)
            path += '/' + get_latest_child_dir(sftp, path)
            path += '/prod217.csv'

            temp_path = 'temp/' + path
            local_path = 'local/' + path

            buckets = b2_api.list_buckets()

            bucket_name = convert_path_to_bucket(path, 'companies')

            existing_buckets = [x for x in buckets if x.name.startswith(bucket_name) and not x.name.endswith('csv')]

            sorted_existing_buckets = sorted(existing_buckets, key=lambda x: int(x.name.split('-')[-1]), reverse=True)

            next_attempt_count = None
            
            if not sorted_existing_buckets:
                print("No buckets exist with name " + bucket_name)
                next_attempt_count = 0
            else:
                latest_attempt_bucket = sorted_existing_buckets[0]
                bucket_info = latest_attempt_bucket.bucket_info
                if ('complete' not in bucket_info or bucket_info['complete'] != 'true'):
                    print("Bucket already exists but is not complete, marking bucket for deletion")
                    latest_attempt_bucket.update(lifecycle_rules=[{"fileNamePrefix": "", "daysFromUploadingToHiding": 1, "daysFromHidingToDeleting": 1}])
                    print("Bucket marked for deletion")
                    next_attempt_count = int(latest_attempt_bucket.name.split('-')[-1]) + 1
                else:
                    print("Bucket already exists and is complete")
                    exit()

            print('creating bucket ' + convert_path_to_bucket(path, 'companies', next_attempt_count))
            bucket = b2_api.create_bucket(convert_path_to_bucket(path, 'companies', next_attempt_count), 'allPrivate')

            # if not os.path.isfile('local/' + path):
            #     os.makedirs(os.path.dirname(temp_path))
            #     sftp.get(path, localpath=temp_path, callback=lambda x,y: print('Downloading: ' + str(x) + ' of ' + str(y) if x % 1000 == 0 else None, end='\r'))            
            #     os.makedirs(os.path.dirname(local_path), exist_ok=True)
            #     os.rename(temp_path, local_path)
            
            file_path = local_path

    url = 'https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-11-01.zip'
    local_zip = 'local/BasicCompanyDataAsOneFile-2023-11-01.zip'
    unzip_dir = 'local/'

    response = requests.get(url, verify=False)
    with open(local_zip, 'wb') as file:
        file.write(response.content)

    with zipfile.ZipFile(local_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    
    file_path = 'local/BasicCompanyDataAsOneFile-2023-11-01.csv'
    output_dir = './output/companies/'
    os.makedirs(output_dir, exist_ok=True)
    with open(file_path) as file:
        csv_reader = csv.reader(file)
        header_row = next(csv_reader)
        trimmed_header_row = [x.strip() for x in header_row]

        csv_dict_reader = csv.DictReader(file, fieldnames=trimmed_header_row)

        for i, row in islice(enumerate(csv_dict_reader), 100):
            company = {
                'company_number': row['CompanyNumber'],
                'company_name': row['CompanyName'],
                'date_of_creation': row['IncorporationDate'],
                'date_of_cessation': row['DissolutionDate'] if row['DissolutionDate'] != "" else None,
                'registered_office_address': {
                    'care_of': row['RegAddress.CareOf'] if row['RegAddress.CareOf'] != "" else None,
                    'po_box': row['RegAddress.POBox'] if row['RegAddress.POBox'] != "" else None,
                    'address_line_1': row['RegAddress.AddressLine1'] if row['RegAddress.AddressLine1'] != "" else None,
                    'address_line_2': row['RegAddress.AddressLine2'] if row['RegAddress.AddressLine2'] != "" else None,
                    'locality': row['RegAddress.PostTown'] if row['RegAddress.PostTown'] != "" else None,
                    'region': row['RegAddress.County'] if row['RegAddress.County'] != "" else None,
                    'postal_code': row['RegAddress.PostCode'] if row['RegAddress.PostCode'] != "" else None,
                    'country': row['RegAddress.Country'] if row['RegAddress.Country'] != "" else None,
                }
            }

            company = {k: v for k, v in company.items() if v is not None}
            company['registered_office_address'] = {k: v for k, v in company['registered_office_address'].items() if v is not None}


            with open(output_dir + row['CompanyNumber'] + '.json', 'w') as f:
                json.dump(company, f)
            
            if i % 1000 == 0:
                print(i)

    counter = 0

    def upload_file(file_name):
        file_path = output_dir + file_name
        bucket.upload_local_file(file_path, file_name)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for _ in executor.map(upload_file, os.listdir(output_dir)):
            counter += 1
            if counter % 1000 == 0:
                print(counter, flush=True)        


    bucket_info = bucket.bucket_info
    bucket_info['complete'] = 'true'
    bucket.set_info(bucket_info)



if __name__ == "__main__":
    main()
