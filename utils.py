from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api, exception
import os


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
        print("Bucket exists")
    except exception.NonExistentBucket:
        print("Bucket doesn't exist, creating")
        bucket = b2_api.create_bucket(bucket_name, "allPrivate")
        print("Bucket created")

    return bucket
