import concurrent.futures
import csv
import json
import os

from dotenv import load_dotenv
from itertools import islice
import sys

from utils import get_companies_bucket, initialise_b2_api


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
                print(f"File {futures[future]} generated an exception: {exc}")
                raise


def main():
    load_dotenv()

    start_index = int(sys.argv[1])
    batch_size = int(sys.argv[2])

    print(
        "Starting at row "
        + str(start_index)
        + " with batch size of "
        + str(batch_size),
    )

    files = os.listdir("artifacts")

    file_path = "artifacts/" + files[0]

    b2_api = initialise_b2_api()

    bucket = get_companies_bucket(b2_api)

    chunk_size = 1000

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
            print(
                f"Processed rows {start_index} through {start_index + len(chunk) - 1}",
            )
            start_index += chunk_size


if __name__ == "__main__":
    main()
