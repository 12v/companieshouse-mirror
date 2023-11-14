from fabric import Connection
import stat
import os
import concurrent.futures
import csv
import shutil
import json
from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api, exception

from dotenv import load_dotenv
import requests
import zipfile
from itertools import islice

load_dotenv()


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


def process_chunk(chunk, bucket):
    def upload_file(row):
        body = json.dumps(generate_json_from_csv(row))
        file_name = row["CompanyNumber"] + ".json"
        bucket.upload_bytes(body.encode("utf-8"), file_name)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(upload_file, row): row["CompanyNumber"] for row in chunk
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(
                    f"File {futures[future]} generated an exception: {exc}", flush=True
                )
                raise


def main():
    if os.path.isdir("temp/"):
        shutil.rmtree("temp/", ignore_errors=True)

    if os.path.isdir("output/"):
        shutil.rmtree("output/", ignore_errors=True)

    file_path = ""

    bucket = None

    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c:
        with c.sftp() as sftp:
            path = get_latest_file(sftp)
            b2_api = initialise_b2_api()

            for bucket in b2_api.list_buckets():
                try:
                    b2_api.delete_bucket(bucket)
                    print("Deleted bucket " + bucket.name, flush=True)
                except Exception as e:
                    print("Couldn't delete bucket " + bucket.name)
                    print(e)

            temp_path = "temp/" + path
            local_path = "local/" + path

            bucket_name = convert_path_to_bucket(path, "companies")

            try:
                bucket = b2_api.get_bucket_by_name(bucket_name)
                print("Bucket exists")
            except exception.NonExistentBucket:
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

    chunk_size = 1000
    total_rows = sum(1 for _ in csv.reader(open(file_path))) - 1

    bucket_info = bucket.bucket_info
    start_index = int(bucket_info.get("progress", 0))
    processed_rows = start_index

    print("Starting at row " + str(start_index) + " of " + str(total_rows), flush=True)

    with open(file_path) as file:
        csv_reader = csv.reader(file)
        header_row = next(csv_reader)
        trimmed_header_row = [x.strip() for x in header_row]

        csv_dict_reader = csv.DictReader(file, fieldnames=trimmed_header_row)

        for _ in range(start_index):
            next(csv_dict_reader, None)

        while True:
            chunk = list(islice(csv_dict_reader, chunk_size))
            if not chunk:
                break

            process_chunk(chunk, bucket)
            processed_rows += len(chunk)
            print(f"Processed {processed_rows} of {total_rows} rows", flush=True)

            bucket_info = bucket.bucket_info
            bucket_info["progress"] = str(processed_rows)
            bucket.set_info(bucket_info)

    bucket_info = bucket.bucket_info
    bucket_info["complete"] = "true"
    bucket.set_info(bucket_info)


if __name__ == "__main__":
    main()
