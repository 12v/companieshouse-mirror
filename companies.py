import csv


def companies_generator(file):
    csv_reader = csv.reader(file)
    header_row = next(csv_reader)
    trimmed_header_row = [x.strip() for x in header_row]

    csv_dict_reader = csv.DictReader(file, fieldnames=trimmed_header_row)

    for row in csv_dict_reader:
        yield generate_company_from_csv(row), row["CompanyNumber"] + ".json"


def generate_company_from_csv(row):
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
