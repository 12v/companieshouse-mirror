def officers_generator(file):
    company = None
    officers = []

    for line in file:
        if is_header_entry(line):
            print(line)
        elif is_trailer_entry(line) or is_company_entry(line):
            if company is not None:
                company["officers"] = officers
                yield company, company["company_number"] + ".json"

            if is_trailer_entry(line):
                print(line)
                return
            elif is_company_entry(line):
                company = generate_company_from_entry(line)
                officers = []
        elif is_officer_entry(line):
            officers.append(generate_officer_from_entry(line))
        else:
            print(line)
            raise Exception("Invalid entry type")


def is_company_entry(line):
    return line[8] == "1"


def is_officer_entry(line):
    return line[8] == "2"


def is_header_entry(line):
    return line[0:8] == "DDDDSNAP"


def is_trailer_entry(line):
    return line[0:8] == "99999999"


def generate_company_from_entry(line):
    return {
        "company_number": line[0:8],
        "company_status": line[9],
        "company_name": line[40:].split("<")[0],
    }


def generate_officer_from_entry(line):
    variable_data = line[76:].split("<")

    officer = {
        "appointment_type": line[10:12],
        "person_number": line[12:24],
        "corporate_indicator": line[24],
        "appointment_date": line[32:40],
        "resignation_date": line[40:48],
        "postcode": line[48:56],
        "partial_date_of_birth": line[56:64],
        "full_date_of_birth": line[64:72],
        "title": variable_data[0],
        "forenames": variable_data[1],
        "surname": variable_data[2],
        "honours": variable_data[3],
        "care_of": variable_data[4],
        "po_box": variable_data[5],
        "address_line_1": variable_data[6],
        "address_line_2": variable_data[7],
        "post_town": variable_data[8],
        "county": variable_data[9],
        "country": variable_data[10],
        "occupation": variable_data[11],
        "nationality": variable_data[12],
        "usual_residential_country": variable_data[13],
    }

    return {k: v for k, v in officer.items() if v != ""}
