try:
    import unzip_requirements
except ImportError:
    pass

import os
import json

try:
    from get_data.helper_get import sap_post_req, write_sap_json, upload_file
except ModuleNotFoundError:
    from helper_get import sap_post_req, write_sap_json, upload_file


"""
Company's ParentId comes from RDS (not G11 from this code) where we associate them in the Athena query

| Workday Field                         | Source Table | Source Field      | Comments                                                                                             |
| ------------------------------------- | ------------ | ----------------- | ---------------------------------------------------------------------------------------------------- |
| Company Reference ID (Must be Unique) | T001         | BUKRS             |                                                                                                      |
| Organization Code                     | T001         | BUKRS             |                                                                                                      |
| Company Name                          | T001         | BUTXT             | will be using short/standard name                                                                    |
| Organization Type Reference           |              |                   | default value "Company"                                                                              |
| Organization Subtype Reference        |              |                   | default value "Company"                                                                              |
| Organization Visibility               |              |                   | default value "Everyone"                                                                             |
| Availability Date                     |              |                   | default to today                                                                                     |
| Include Organization Code In Name     |              |                   | default value "false"                                                                                |
| Country ISO Code                      | T001         | LAND1             | not sent, but queried for athena                                                                     |
| Container Organization Reference      | RDS          | PARENTID          | not sent, but queried from company hierarchies in athena                                             |
| Relevant to People Cost FLAG          | T001Z        | PARTY/PAVAL/BUKRS | will be filtered within the SAP PO query to only return BUKRS where PARTY = “SAPERS” and PAVAL = “Y” |

"""


def get_companies(local=False):
    """get the list of all Company Codes from T001"""

    header_mapping = {
        "BUKRS": "id",
        "BUTXT": "name",
        "WAERS": "currencycode",
        "LAND1": "country",
    }
    cpny_table_name = "T001"
    cpny_select_fields = "BUKRS,BUTXT,WAERS,LAND1"  # SPRAS,ZZCCTYPID,ZZCCLNAME,ORT01
    cpny_where_field = ""

    return sap_post_req(
        cpny_table_name, cpny_select_fields, cpny_where_field, header_mapping, local
    )


def get_companies_to_keep(local=False):
    """get the list of Company Codes to keep from T001Z"""

    code_table_name = "T001Z"
    code_select_fields = "BUKRS"
    code_where_field = "PARTY eq 'SAPERS' and PAVAL eq 'Y'"
    cpny_code_rows = sap_post_req(
        code_table_name, code_select_fields, code_where_field, None, local
    )
    return [cpny_codes["BUKRS"] for cpny_codes in cpny_code_rows]


def lambda_handler(event, context):

    # get environment variables
    bucket_name = os.environ.get("BUCKET")
    bucket_prefix = os.environ.get("COMPANYCODE_NEW")

    # get the list of all Company Codes from T001
    cpny_rows = get_companies(event == "")

    # get the list of Company Codes that we want to keep from T001Z
    cpny_code_list = get_companies_to_keep(event == "")

    # only keep the Buildings that match a Site (which are already selected based on Flag)
    results = []
    failures = []
    for row in cpny_rows:
        cpny_id = row["id"]

        if cpny_id in cpny_code_list:

            # we're removing currency code
            row.pop("currencycode", None)

            results.append(row)

    try:
        mode = event["mode"]
        file_name = "MANUAL_TESTING_companycode.json"
    except KeyError:
        file_name = "companycode.json"

    output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"
    write_sap_json(results, output_path)

    upload_file(output_path, bucket_name, bucket_prefix, file_name)


# if __name__ == "__main__":
#     with open("environ.json", "r") as f:
#         environ = json.loads(f.read())
#     os.environ["SECRETS"] = environ["SECRETS"]
#     os.environ["BUCKET"] = environ["BUCKET"]
#     os.environ["COMPANYCODE_NEW"] = environ["COMPANYCODE_NEW"]
#     event = {"mode": "manual"}
#     lambda_handler(event, "")
