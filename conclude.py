import sys
from dotenv import load_dotenv
from utils import initialise_b2_api, get_companies_bucket


def main():
    load_dotenv()

    file_name = sys.argv[1]

    b2_api = initialise_b2_api()

    bucket = get_companies_bucket(b2_api)

    bucket_info = bucket.bucket_info
    bucket_info[file_name.lower()] = "true"
    bucket.set_info(bucket_info)


if __name__ == "__main__":
    main()
