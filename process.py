import concurrent.futures
import csv
import json
import os
import argparse

from dotenv import load_dotenv
from itertools import islice

from utils import get_bucket, initialise_b2_api

artifacts_dir = "artifacts"


def main(key, offset, batch_size, type):
    load_dotenv()

    b2_api = initialise_b2_api()

    file_name = offset.split("::")[0]
    start_index = int(offset.split("::")[1])

    print(
        "Processing file "
        + file_name
        + " at row "
        + str(start_index)
        + " with batch size of "
        + str(batch_size),
        flush=True,
    )

    keys = os.listdir(artifacts_dir)
    print("Contents of artifacts directory: " + str(keys), flush=True)

    key_dir = os.path.join(artifacts_dir, key)
    files = os.listdir(key_dir)
    print("Contents of " + key_dir + " directory: " + str(files), flush=True)

    file_path = os.path.join(key_dir, file_name)

    bucket = get_bucket(b2_api, type)

    chunk_size = 1000

    with open(file_path) as file:
        csv_reader = csv.reader(file)
        header_row = next(csv_reader)
        trimmed_header_row = [x.strip() for x in header_row]

        csv_dict_reader = csv.DictReader(file, fieldnames=trimmed_header_row)

        for _ in range(start_index):
            next(csv_dict_reader, None)

        batch = islice(csv_dict_reader, batch_size)

        while True:
            chunk = list(islice(batch, chunk_size))
            if not chunk:
                break

            process_chunk(chunk, bucket)
            print(
                f"Processed rows {start_index} through {start_index + len(chunk) - 1}",
                flush=True,
            )
            start_index += chunk_size


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

    return company


def process_chunk(chunk, bucket):
    def upload_file(row):
        body = json.dumps(generate_json_from_csv(row))
        file_name = row["CompanyNumber"] + ".json"
        bucket.upload_bytes(body.encode("utf-8"), file_name)

    with concurrent.futures.ThreadPoolExecutor(10) as executor:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", required=True)
    parser.add_argument("--offset", required=True)
    parser.add_argument("--batch_size", required=True)
    parser.add_argument("--type", required=True)
    args = parser.parse_args()
    main(args.key, args.offset, int(args.batch_size), args.type)
