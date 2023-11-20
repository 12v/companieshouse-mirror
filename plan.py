import os
import shutil
from fabric import Connection
import requests
import zipfile
from dotenv import load_dotenv
import json
import argparse

from utils import is_url, set_output

artifacts_dir = "artifacts"
batch_size = 100000
file_extension_to_header_row_count = {".csv": 1, ".dat": 2}


def main(key, path):
    load_dotenv()

    set_output("batch_size", str(batch_size))

    artifacts_subdirectory = os.path.join(artifacts_dir, key)
    if not os.path.isdir(artifacts_subdirectory):
        os.makedirs(artifacts_subdirectory, exist_ok=True)

        if is_url(path):
            download_from_url(artifacts_subdirectory, path)
        else:
            download_from_sftp(artifacts_subdirectory, path)

    matrix = create_matrix(artifacts_subdirectory)
    set_output("matrix", json.dumps(matrix))


def create_matrix(directory):
    matrix = []

    for file in os.listdir(directory):
        with open(os.path.join(directory, file), "r") as f:
            header_row_count = file_extension_to_header_row_count[
                os.path.splitext(file)[1]
            ]
            file_line_count = sum(1 for line in f if line.strip()) - header_row_count

            matrix.extend(
                [
                    file + "::" + str(offset)
                    for offset in range(0, file_line_count, batch_size)
                ]
            )

    return matrix


def download_from_url(directory, path):
    local_zip = "download"

    for _ in range(3):
        try:
            requests.get(path, local_zip, verify=False)
            break
        except requests.exceptions.ConnectTimeout as e:
            print(e, flush=True)
            continue
    else:
        print("Failed to download the file after 3 attempts.")
        exit(1)

    response = requests.get(path, verify=False)
    with open(local_zip, "wb") as file:
        file.write(response.content)

    with zipfile.ZipFile(local_zip, "r") as zip_ref:
        zip_ref.extractall(directory)

    os.remove(local_zip)


def download_from_sftp(directory, path):
    with Connection(
        os.getenv("CH_URL"),
        user=os.getenv("CH_USER"),
        connect_kwargs={"key_filename": os.getenv("SSH_KEY_PATH")},
    ) as c, c.sftp() as sftp:
        with c.sftp() as sftp:
            temp_dir = "temp"

            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

            os.mkdir(temp_dir)
            for file in sftp.listdir(path):
                full_sftp_path = os.path.join(path, file)
                full_temp_path = os.path.join(temp_dir, file)
                print(
                    "Downloading " + full_sftp_path + " to " + full_temp_path,
                    flush=True,
                )
                sftp.get(
                    full_sftp_path,
                    localpath=full_temp_path,
                    callback=lambda x, y: print(
                        "Downloading: " + str(x) + " of " + str(y)
                        if x % 1000 == 0
                        else None,
                        end="\r",
                    ),
                )
                print(
                    "Downloaded " + full_sftp_path + " to " + full_temp_path, flush=True
                )

            for file in os.listdir(temp_dir):
                os.rename(os.path.join(temp_dir, file), os.path.join(directory, file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", required=True)
    parser.add_argument("--path", required=True)
    args = parser.parse_args()

    main(args.key, args.path)
