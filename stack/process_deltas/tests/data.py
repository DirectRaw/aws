import os

TENANT = "https://wd5-impl-services1.workday.com/ccx/service/pg5/Human_Resources/v34.1"
TENANT_FM = (
    "https://wd5-impl-services1.workday.com/ccx/service/pg4/Financial_Management/v34.2"
)

HIERARCHIES = [
    "onezeroone",
    "ninetwoone",
    "ninetwosix",
    "building",
    "site",
    "costcenter",
    "companycode",
]

COMPANY_HIERARCHY = {
    "Inactive": "N",
    "id": "000",
    "parentid": "000",
    "name": "COMPANY HIERARCHY NAME",
    "rootflag": "N",
    "lvl": "0",
    "parentlvl": "0",
    "parentname": "PARENT NAME",
}

COMPANY = {"Inactive": "N", "id": "000", "name": "COMPANY NAME", "parentid": "000"}

COSTCENTER_HIERARCHY = {
    "Inactive": "N",
    "id": "CCSS000000",
    "parentid": "CCSS000000",
    "name": "COST CENTER HIERARCHY NAME",
    "lvl": "0",
    "parentlvl": "0",
    "rootflag": "N",
    "parentname": "PARENT NAME",
    "companycode": "000",
    "tdcindicator": "A",
    "country": "US",
    "status": "A",
}

COSTCENTER = {
    "Inactive": "N",
    "id": "CCSS000000",
    "parentid": "CCSS000000",
    "name": "COST CENTER NAME",
    "rootflag": "Y",
    "lvl": "0",
    "parentlvl": "0",
    "parentname": "PARENT NAME",
    "companycode": "123",
    "tdcindicator": "abc",
}

LOCATION_HIERARCHY = {
    "Inactive": "N",
    "id": "0000",
    "parentid": "0000",
    "name": "LOCATION HIERARCHY NAME",
    "lvl": "0",
    "parentlvl": "0",
    "rootflag": "N",
    "parentname": "PARENT NAME",
    "status": "A",
}

SITE = {
    "id": "0000",
    "name": "SITE NAME",
    "status": "A",
    "location_type_id": "TYPE",
    "country": "US",
    "address_line_1": "ADDRESS LINE 1",
    "address_line_2": "ADDRESS LINE 2",
    "municipality": "CITY",
    "region": "SU",
    "postal_code": "00000",
    "user_language_id": "en_US",
    "location_usage_type": "BUSINESS SITE",
    "parentid": "0000",
    "default_currency_id": "USD",
    "time_profile_id": "Standard_Hours_40",
    "region_subdivision_1": "REGION SUBDIVISION 1",
    "city_subdivision_1": "CITY SUBDIVISION 1",
}

BUILDING = {
    "id": "000000",
    "name": "BUILDING NAME",
    "country": "EG",
    "location_usage_type": "WORK SPACE",
    "location_type_id": "B",
    "parentid": "0000",
    "time_profile_id": "Standard_Hours_40",
    "default_currency_id": "USD",
}


class Context:
    def __init__(self):
        self.aws_request_id = "request_id"
        self.log_group_name = "log_group_name"
        self.log_stream_name = "log_stream_name"


context = Context()


def get_local_file(file):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"zeep/{file}")
    return path


def get_addr_components():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "addr_components.json"
    )


def get_xml(file):
    path = get_local_file(file)
    with open(path, "r") as f:
        return f.read().strip()
