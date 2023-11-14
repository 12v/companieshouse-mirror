from fabric import Connection
import stat
import os
import concurrent.futures
import csv
import shutil
import json
from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api

from dotenv import load_dotenv
import requests
import zipfile

load_dotenv()

bucket = None


def print_contents(sftp):
    files = sftp.listdir()
    print("Current directory: {}".format(sftp.getcwd()))
    for file in files:
        print(file)


def get_latest_child_dir(sftp, path):
    dirs = [x.filename for x in sftp.listdir_attr(path) if stat.S_ISDIR(x.st_mode)]
    return sorted(dirs, reverse=True)[0]


def convert_path_to_bucket(path, prefix, suffix=None):
    name = prefix + "-" + path.replace("/", "-").replace(".", "-")
    return name if suffix is None else name + "-" + str(suffix)


def initialise_b2_api():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    application_key_id = os.getenv("B2_APPLICATION_KEY_ID")
    application_key = os.getenv("B2_APPLICATION_KEY")
    b2_api.authorize_account("production", application_key_id, application_key)
    return b2_api


def get_latest_file(sftp):
    path = "free/prod217"
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/prod217.csv"
    return path


def generate_json_from_csv(row):
    company = {
        "company_number": row["CompanyNumber"],
        "company_name": row["CompanyName"],
        "date_of_creation": row["IncorporationDate"],
        "date_of_cessation": row["DissolutionDate"]
        if row["DissolutionDate"] != ""
        else None,
        "registered_office_address": {
            "care_of": row["RegAddress.CareOf"]
            if row["RegAddress.CareOf"] != ""
            else None,
            "po_box": row["RegAddress.POBox"]
            if row["RegAddress.POBox"] != ""
            else None,
            "address_line_1": row["RegAddress.AddressLine1"]
            if row["RegAddress.AddressLine1"] != ""
            else None,
            "address_line_2": row["RegAddress.AddressLine2"]
            if row["RegAddress.AddressLine2"] != ""
            else None,
            "locality": row["RegAddress.PostTown"]
            if row["RegAddress.PostTown"] != ""
            else None,
            "region": row["RegAddress.County"]
            if row["RegAddress.County"] != ""
            else None,
            "postal_code": row["RegAddress.PostCode"]
            if row["RegAddress.PostCode"] != ""
            else None,
            "country": row["RegAddress.Country"]
            if row["RegAddress.Country"] != ""
            else None,
        },
    }

    company = {k: v for k, v in company.items() if v is not None}
    company["registered_office_address"] = {
        k: v for k, v in company["registered_office_address"].items() if v is not None
    }


output_dir = "./output/companies/"


def upload_file(file_name):
    file_path = output_dir + file_name
    bucket.upload_local_file(file_path, file_name)


def process_chunk(chunk, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for row in chunk:
        with open(output_dir + row["CompanyNumber"] + ".json", "w") as f:
            json.dump(generate_json_from_csv(row), f)

    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.map(upload_file, os.listdir(output_dir))

    for file_name in os.listdir(output_dir):
        upload_file(file_name)

    shutil.rmtree(output_dir, ignore_errors=True)


def main():
    if os.path.isdir("temp/"):
        shutil.rmtree("temp/", ignore_errors=True)

    if os.path.isdir("output/"):
        shutil.rmtree("output/", ignore_errors=True)

    file_path = ""

    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c:
        with c.sftp() as sftp:
            path = get_latest_file(sftp)
            b2_api = initialise_b2_api()

            temp_path = "temp/" + path
            local_path = "local/" + path

            buckets = b2_api.list_buckets()

            bucket_name = convert_path_to_bucket(path, "companies")

            try:
                bucket = b2_api.get_bucket_by_name(bucket_name)
                print("Bucket exists")
            except b2sdk.exception.NonExistentBucket:
                print("Bucket doesn't exist, creating")
                bucket = b2_api.create_bucket(bucket_name, "allPrivate")
                print("Bucket created")

            bucket_info = bucket.bucket_info
            if "complete" not in bucket_info or bucket_info["complete"] != "true":
                print("Bucket exists but is not complete, continuing")
            else:
                print("Bucket already exists and is complete")
                exit()

            # if not os.path.isfile('local/' + path):
            #     os.makedirs(os.path.dirname(temp_path))
            #     sftp.get(path, localpath=temp_path, callback=lambda x,y: print('Downloading: ' + str(x) + ' of ' + str(y) if x % 1000 == 0 else None, end='\r'))
            #     os.makedirs(os.path.dirname(local_path), exist_ok=True)
            #     os.rename(temp_path, local_path)

            file_path = local_path

    file_path = "BasicCompanyDataAsOneFile-2023-11-01.csv"

    if not os.path.isfile(file_path):
        url = "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-11-01.zip"
        local_zip = "BasicCompanyDataAsOneFile-2023-11-01.zip"
        response = requests.get(url, verify=False)
        with open(local_zip, "wb") as file:
            file.write(response.content)

        with zipfile.ZipFile(local_zip, "r") as zip_ref:
            zip_ref.extractall()

        os.remove(local_zip)

    chunk = []
    with open(file_path) as file:
        csv_reader = csv.reader(file)
        header_row = next(csv_reader)
        trimmed_header_row = [x.strip() for x in header_row]

        csv_dict_reader = csv.DictReader(file, fieldnames=trimmed_header_row)

        for i, row in enumerate(csv_dict_reader):
            chunk.append(row)
            if (i + 1) % 1000 == 0:
                print(i, flush=True)
                process_chunk(chunk, output_dir)
                chunk = []
        if chunk:
            process_chunk(chunk, output_dir)

    # bucket_info = bucket.bucket_info
    # bucket_info["complete"] = "true"
    # bucket.set_info(bucket_info)


if __name__ == "__main__":
    main()
