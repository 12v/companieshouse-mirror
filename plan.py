import os
import sys
import shutil
from fabric import Connection
import requests
import stat
import zipfile
from dotenv import load_dotenv
import json

from utils import initialise_b2_api, get_companies_bucket


def main():
    load_dotenv()

    b2_api = initialise_b2_api()

    delete_empty_buckets(b2_api)

    os.makedirs("artifacts/", exist_ok=True)

    batch_size = 100000

    set_output("batch_size", str(batch_size))

    artifacts_dir = "artifacts"

    path = find_latest_directory("prod217")

    override = False

    if len(sys.argv[1]) > 0:
        path = sys.argv[1]
        override = True

    set_output("file_name", path)

    if is_batch_processed(b2_api, path):
        print("Batch " + path + " is processed, exiting", flush=True)
        set_output("matrix", "[]")
        exit()
    else:
        print("Batch " + path + " is not processed, continuing", flush=True)

    if override:
        if not os.path.isdir(os.path.join(artifacts_dir, path)):
            download_from_override(sys.argv[1])
    else:
        if not os.path.isdir(os.path.join(artifacts_dir, path)):
            download_from_sftp(path)

    file_path = os.path.join(artifacts_dir, file_name)
    file_name = "BasicCompanyDataAsOneFile-2023-11-01.csv"

    file_line_count = sum(1 for _ in open(file_path)) - 2
    matrix = json.dumps(list(range(0, file_line_count, batch_size)))
    set_output("matrix", matrix)


def download_from_override():
    url = "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-11-01.zip"
    local_zip = "download.zip"
    response = requests.get(url, verify=False)
    with open(local_zip, "wb") as file:
        file.write(response.content)

    with zipfile.ZipFile(local_zip, "r") as zip_ref:
        zip_ref.extractall("artifacts")

    os.remove(local_zip)


def is_batch_processed(b2_api, key):
    bucket = get_companies_bucket(b2_api)

    bucket_info = bucket.bucket_info
    print("Bucket info: " + str(bucket_info), flush=True)

    return key.lower() not in bucket_info or bucket_info[key.lower()] != "true"


def download_from_sftp(path):
    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c, c.sftp() as sftp:
        with c.sftp() as sftp:
            if os.path.isdir("temp/"):
                shutil.rmtree("temp/", ignore_errors=True)

            os.mkdir("temp")
            for file in sftp.listdir(path):
                full_sftp_path = os.path.join(path, file)
                print("Downloading " + full_sftp_path, flush=True)
                sftp.get(
                    full_sftp_path,
                    localpath=os.path.join("temp", file),
                    callback=lambda x, y: print(
                        "Downloading: " + str(x) + " of " + str(y)
                        if x % 1000 == 0
                        else None,
                        end="\r",
                    ),
                )
                print("Downloaded " + full_sftp_path, flush=True)

            for file in os.listdir("temp"):
                os.rename(os.path.join("temp", file), os.path.join("artifacts", file))


def find_latest_directory(product):
    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c:
        with c.sftp() as sftp:
            path = "free/" + product
            # year
            path += "/" + get_latest_child_dir(sftp, path)
            # month
            path += "/" + get_latest_child_dir(sftp, path)
            # day
            path += "/" + get_latest_child_dir(sftp, path)

            print("Found latest directory: " + path, flush=True)

            return path


def delete_empty_buckets(b2_api):
    for bucket in b2_api.list_buckets():
        print("Deleting bucket " + bucket.name, flush=True)
    try:
        b2_api.delete_bucket(bucket)
        print("Deleted bucket " + bucket.name, flush=True)
    except Exception as e:
        print(e)


def get_latest_child_dir(sftp, path):
    dirs = [x.filename for x in sftp.listdir_attr(path) if stat.S_ISDIR(x.st_mode)]
    return sorted(dirs, reverse=True)[0]


def set_output(name, value):
    with open(os.getenv("GITHUB_OUTPUT", "/dev/null"), "a") as f:
        f.write(f"{name}={value}\n")
    print("Setting output " + name + " to " + value, flush=True)


if __name__ == "__main__":
    main()
