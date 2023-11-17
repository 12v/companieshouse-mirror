from dotenv import load_dotenv
from utils import initialise_b2_api, get_companies_bucket
import argparse


def main(key):
    load_dotenv()

    b2_api = initialise_b2_api()

    bucket = get_companies_bucket(b2_api)

    bucket_info = bucket.bucket_info
    bucket_info[key] = "true"
    bucket.set_info(bucket_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", required=True)
    args = parser.parse_args()
    main(args.key)
