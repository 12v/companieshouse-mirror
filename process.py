import concurrent.futures
import json
import os
import argparse

from dotenv import load_dotenv
from itertools import islice

from utils import get_bucket, initialise_b2_api
from companies import companies_generator
from officers import officers_generator

artifacts_dir = "artifacts"


def main(key, type):
    load_dotenv()

    # b2_api = initialise_b2_api()

    # file_name = offset.split("::")[0]
    # start_index = int(offset.split("::")[1])

    # print(
    #     "Processing file "
    #     + file_name
    #     + " at row "
    #     + str(start_index)
    #     + " with batch size of "
    #     + str(batch_size),
    #     flush=True,
    # )

    keys = os.listdir(artifacts_dir)
    print("Contents of artifacts directory: " + str(keys), flush=True)

    key_dir = os.path.join(artifacts_dir, key)
    files = os.listdir(key_dir)
    print("Contents of " + key_dir + " directory: " + str(files), flush=True)

    for file_name in files:
        file_path = os.path.join(key_dir, file_name)

        # bucket = get_bucket(b2_api, type)

        # chunk_size = 1000

        new_dir = os.path.join(artifacts_dir, key + "_json")
        os.makedirs(new_dir)

        generator = None

        if type == "officers":
            generator = officers_generator
        elif type == "companies":
            generator = companies_generator
        else:
            raise Exception("Invalid type: " + type)

        with open(file_path) as file:
            for i, (company, file_name) in enumerate(generator(file)):
                with open(os.path.join(new_dir, file_name), "w") as f:
                    f.write(json.dumps(company))

                if i % 10000 == 0:
                    print("Processed " + str(i) + " rows", flush=True)

            # for _ in range(start_index):
            #     next(csv_dict_reader, None)

            # batch = islice(csv_dict_reader, batch_size)

            # while True:
            #     chunk = list(islice(batch, chunk_size))
            #     if not chunk:
            #         break

            #     process_chunk(chunk, bucket)
            #     print(
            #         f"Processed rows {start_index} through {start_index + len(chunk) - 1}",
            #         flush=True,
            #     )
            #     start_index += chunk_size


# def process_chunk(chunk, bucket):
#     def upload_file(row):
#         body = json.dumps(generate_json_from_csv(row))
#         file_name = row["CompanyNumber"] + ".json"
#         bucket.upload_bytes(body.encode("utf-8"), file_name)

#     with concurrent.futures.ThreadPoolExecutor(10) as executor:
#         futures = {
#             executor.submit(upload_file, row): row["CompanyNumber"] for row in chunk
#         }
#         for future in concurrent.futures.as_completed(futures):
#             try:
#                 future.result()
#             except Exception as exc:
#                 print(
#                     f"File {futures[future]} generated an exception: {exc}", flush=True
#                 )
#                 raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", required=True)
    parser.add_argument("--type", required=True)
    args = parser.parse_args()
    main(args.key, args.type)
