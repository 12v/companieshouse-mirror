import os
from fabric import Connection
import requests
import stat
import zipfile
from dotenv import load_dotenv
import json

from utils import initialise_b2_api, get_companies_bucket


def get_latest_child_dir(sftp, path):
    dirs = [x.filename for x in sftp.listdir_attr(path) if stat.S_ISDIR(x.st_mode)]
    return sorted(dirs, reverse=True)[0]


def get_latest_file(sftp):
    path = "free/prod217"
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/" + get_latest_child_dir(sftp, path)
    path += "/prod217.csv"
    return path


def print_contents(sftp):
    files = sftp.listdir()
    print("Current directory: {}".format(sftp.getcwd()))
    for file in files:
        print(file)


def set_output(name, value):
    print("Setting output " + name + " to " + value)
    print("::set-output name=" + name + "::" + value)


def main():
    load_dotenv()

    # if os.path.isdir("temp/"):
    #     shutil.rmtree("temp/", ignore_errors=True)

    b2_api = initialise_b2_api()

    # for bucket in b2_api.list_buckets():
    #     print("Deleting bucket " + bucket.name)
    # try:
    #     b2_api.delete_bucket(bucket)
    #     print("Deleted bucket " + bucket.name)
    # except Exception as e:
    #     print(e)

    os.makedirs("artifacts/", exist_ok=True)

    batch_size = 100000

    set_output("batch_size", str(batch_size))

    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c:
        with c.sftp() as sftp:
            path = get_latest_file(sftp)

            print("Found latest file: " + path)

            # temp_path = "temp/" + path
            file_path = "local/" + path

            file_name = "BasicCompanyDataAsOneFile-2023-11-01.csv"

            set_output("file_name", file_name)

            file_path = "artifacts/" + file_name

            bucket = get_companies_bucket(b2_api)

            bucket_info = bucket.bucket_info
            if file_name not in bucket_info or bucket_info[file_name] != "true":
                print("Bucket exists but is not complete, continuing")
            else:
                print("Bucket already exists and is complete")
                set_output("matrix", "[]")
                exit()

            # if not os.path.isfile('local/' + path):
            #     os.makedirs(os.path.dirname(temp_path))
            #     sftp.get(path, localpath=temp_path, callback=lambda x,y: print('Downloading: ' + str(x) + ' of ' + str(y) if x % 1000 == 0 else None, end='\r'))
            #     os.makedirs(os.path.dirname(file_path), exist_ok=True)
            #     os.rename(temp_path, file_path)

            if not os.path.isfile(file_path):
                url = "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-11-01.zip"
                local_zip = "BasicCompanyDataAsOneFile-2023-11-01.zip"
                response = requests.get(url, verify=False)
                with open(local_zip, "wb") as file:
                    file.write(response.content)

                with zipfile.ZipFile(local_zip, "r") as zip_ref:
                    zip_ref.extractall("artifacts")

                os.remove(local_zip)

            file_line_count = sum(1 for _ in open(file_path)) - 2

            matrix = json.dumps(list(range(0, file_line_count, batch_size)))
            set_output("matrix", matrix)


if __name__ == "__main__":
    main()
