import os
from fabric import Connection
import stat
from dotenv import load_dotenv
import argparse

from utils import initialise_b2_api, get_bucket, set_output, is_url


def main(product, type):
    load_dotenv()

    b2_api = initialise_b2_api()

    delete_empty_buckets(b2_api)

    url_override = is_url(product)

    if not url_override:
        product = find_latest_directory(product)

    key = product.lower().replace("/", "-").replace(":", "-")

    if is_batch_processed(b2_api, key, type):
        print("Batch " + key + " is processed, exiting", flush=True)
        exit()
    else:
        print("Batch " + key + " is not processed, continuing", flush=True)

    set_output("key", key)
    set_output("path", product)


def is_batch_processed(b2_api, key, type):
    bucket = get_bucket(b2_api, type)

    bucket_info = bucket.bucket_info
    print("Bucket info: " + str(bucket_info), flush=True)

    return key in bucket_info and bucket_info[key] == "true"


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", required=True)
    parser.add_argument("--type", required=True)
    args = parser.parse_args()

    main(args.product, args.type)
