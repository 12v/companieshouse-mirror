from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api, exception
import os
from urllib.parse import urlparse


def initialise_b2_api():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    application_key_id = os.getenv("B2_APPLICATION_KEY_ID")
    application_key = os.getenv("B2_APPLICATION_KEY")
    b2_api.authorize_account("production", application_key_id, application_key)
    return b2_api


def get_companies_bucket(b2_api):
    bucket_name = "sugartrail-companies"

    try:
        bucket = b2_api.get_bucket_by_name(bucket_name)
        print("Bucket with name " + bucket_name + " found", flush=True)
    except exception.NonExistentBucket:
        print("Bucket with name " + bucket_name + " not found", flush=True)
        bucket = b2_api.create_bucket(bucket_name, "allPrivate")
        print("Bucket with name " + bucket_name + " created", flush=True)

    return bucket


def set_output(name, value):
    with open(os.getenv("GITHUB_OUTPUT", "/dev/null"), "a") as f:
        f.write(f"{name}={value}\n")
    print("Setting output " + name + " to " + value, flush=True)


def is_url(string):
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
