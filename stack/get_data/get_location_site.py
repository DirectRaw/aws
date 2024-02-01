try:
    import unzip_requirements
except ImportError:
    pass

import os
import json
import boto3

try:
    from get_data.helper_get import sap_post_req, write_sap_json, upload_file
except ModuleNotFoundError:
    from helper_get import sap_post_req, write_sap_json, upload_file

"""
921 – HR GEO HIER WITH SITE-HYBRID 

The hierarchy is designed to go all the way down to site level, combining the 705 Geography hierarchy with T001W 
where the Building Reference Flag is checked (So Sites where Employees could be assigned).  Additionally we will 
filter the loading of Sites into Location by the same Building Reference Flag in T001W (ZZBLDG_REF). 

| Workday Field                | Source Table    | Source Field                      | Comments                                                                              |
| ---------------------------- | --------------- | --------------------------------- | ------------------------------------------------------------------------------------- |
| Location Reference ID        | T001W           | WERKS                             |                                                                                       |
| Location Name                | T001W           | NAME1                             |                                                                                       |
| Location Usage               | -               | Default to “BUSINESS SITE”        |                                                                                       |
| Location Type                | T001W           | ZZSITETYPC                        | directly passing what is in ZZSITETYPC, ID Type needs to be in WD                     |
| Default Hours per Week       | T001W           | Default value “STANDARD_HOURS_40” | get from existing one in workday or set default 40 hours                              |
| Country ISO Code             | T001W           | LAND1                             |                                                                                       |
| Address Line #1              | T001W           | STRAS                             | conditionally sent                                                                    |
| Address Line #2              |                 |                                   | based on manual fixes                                                                 |
| Municipality (City)          | T001W           | ORT01                             | conditionally sent                                                                    |
| Submunicipality 1            | ADRC            | STR_SUPPL3                        | conditionally sent (City_Subdivision_1)                                               |
| Submunicipality 2            | -               |                                   | not sent                                                                              |
| State/Province/Region        | T001W           | REGIO                             | conditionally sent, only populate home address data block if use employee address = N |
| County/Subregion 1           | ADRC            | CITY2                             | conditionally sent (Region_Subdivision_1)                                             |
| County/Subregion 2           | -               |                                   | not sent                                                                              |
| Postal Code                  | T001W           | PSTLZ                             | conditionally sent                                                                    |
| Currency Code                | T005            | WAERS                             | not set, automatically done by WD based on country                                    |
| Display Language             | T001W           | SPRAS                             | default to EN, not recommended through integration                                    |
| Location Hierarchy Reference | T001W           | WERKS                             | "LH" + WERKS (this is done in Athena)                                                 |
| Communication Usage Type ID  |                 |                                   | default to "BUSINESS"                                                                 |
| Inactive                     | T001W / Workday | ZZSITESTCD / Inactive             | default to "false" because we are checking G11 and Workday and sending active sites   |
"""


def get_t001w(local=False):

    field_mapping_t001w = {
        "WERKS": "ID",  # Location_ID
        "NAME1": "Name",  # Location_Name
        "ZZSITETYPC": "Location_Type_ID",
        "LAND1": "Country",  # ISO 2 (Country_Reference_ID)
        "STRAS": "Address_Line_1",
        "ORT01": "Municipality",
        "REGIO": "Region",  # Country_Region_ID
        "PSTLZ": "Postal_Code",
        "SPRAS": "User_Language_ID",  # default to english
        "ZZSITESTCD": "status",
        "ADRNR": "join_field",
    }

    site_table_name = "T001W"
    site_select_fields = (
        "WERKS,NAME1,ZZSITETYPC,LAND1,STRAS,ORT01,REGIO,PSTLZ,SPRAS,ADRNR,ZZSITESTCD"
    )
    site_where_field = "ZZBLDG_REF EQ 'X'"

    return sap_post_req(
        site_table_name, site_select_fields, site_where_field, field_mapping_t001w, local,
    )


def get_adrc(local=False):

    field_mapping_adrc = {
        "STR_SUPPL3": "City_Subdivision_1",
        "CITY2": "Region_Subdivision_1",
        "ADDRNUMBER": "join_field",
    }
    adrc_table_name = "ADRC"
    adrc_select_fields = "ADDRNUMBER,STR_SUPPL3,CITY2"
    adrc_where_field = "ADDR_GROUP EQ 'CA01'"

    adrc_response = sap_post_req(
        adrc_table_name, adrc_select_fields, adrc_where_field, field_mapping_adrc, local
    )

    write_sap_json(adrc_response, "/tmp/adrc_response.json")

    # process ADRC repsonse so the key becomes the ADDRNUMBER field to join with sites
    results = {}
    for row in adrc_response:
        results[row["join_field"]] = {
            "City_Subdivision_1": f"{row['City_Subdivision_1']}",
            "Region_Subdivision_1": f"{row['Region_Subdivision_1']}",
        }

    return results


def lambda_handler(event, context):

    # get environment variables
    data_bucket_name = os.environ.get("BUCKET")
    data_bucket_prefix = os.environ.get("SITE_NEW")

    # Make request to T001W
    site_rows = get_t001w(event == "")
    adrc_rows = get_adrc(event == "")

    # process results
    adrc_failures = []

    for row in site_rows:

        # populate non address related data and defaults first
        row["Location_Usage_Type"] = "BUSINESS SITE"
        row["User_Language_ID"] = "en_US"
        # Location_Hierarchy_Reference
        row["ParentId"] = row["ID"]
        row["Default_Currency_ID"] = ""
        row["Time_Profile_ID"] = ""
        row["Address_Line_2"] = ""

        try:
            # we are getting these two fields from ADRC
            joined_fields = adrc_rows[row["join_field"]]
            row["Region_Subdivision_1"] = joined_fields["Region_Subdivision_1"]
            row["City_Subdivision_1"] = joined_fields["City_Subdivision_1"]
        except KeyError:
            adrc_failures.append(row)
            row["Region_Subdivision_1"] = ""
            row["City_Subdivision_1"] = ""
        finally:
            # remove the field that we are using to join with adrc
            row.pop("join_field", None)

    if len(adrc_failures):
        # we don't do anything with this, but these are the ones from T001W which don't exists in ADRC
        file_name = "adrc_failures.json"
        output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"
        write_sap_json(adrc_failures, output_path)

    # write to file locally and upload to s3
    file_name = "site.json"
    output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"

    write_sap_json(site_rows, output_path)

    upload_file(output_path, data_bucket_name, data_bucket_prefix, file_name)

    return {"result_count": len(site_rows)}


# if __name__ == "__main__":
#     with open("environ_dev.json", "r") as f:
#         environ = json.loads(f.read())
#     os.environ["SECRETS"] = environ["SECRETS"]
#     os.environ["BUCKET"] = environ["BUCKET"]
#     os.environ["SITE_NEW"] = environ["SITE_NEW"]
#     lambda_handler("", "")
