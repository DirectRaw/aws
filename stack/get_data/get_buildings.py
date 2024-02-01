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
All buildings associated with sites that match the site (Location) filter criteria 
should be included (where Site Building Reference flag is ticked “X”). 

first 4 digits of GERNR from ZTXXBLDG need to match WERKS from T001W
    so where ZTXXETBLDG-->GEBNR[:4] == TOO1W-->WERKS

| Workday Field                          | Source Table | Source Field | Comments                                                      |
| -------------------------------------- | ------------ | ------------ | ------------------------------------------------------------- |
| Location Reference ID (Must be Unique) | ZTXXETBLDG   | GEBNR        |                                                               |
| Location Name                          | ZTXXETBLDG   | ZBLDGNAME    |                                                               |
| Location Usage                         |              |              | default to “WORK SPACE”                                       |
| Location Type                          |              |              | default to "B"                                                |
| Default Hours per Week                 |              |              | not sent                                                      |
| Country ISO Code                       | ZTXXETBLDG   | LAND1        | not sent                                                      |
| Currency Code                          |              |              | not sent                                                      |
| Superior Location Reference            |              |              | derive from first 4 digits of GEBNR                           |
| Inactive                               | Workday      |              | check in Workday, default to "false" if creating new building |
"""


def get_buildings(local=False):
    header_mapping = {
        "GEBNR": "id",
        "ZBLDGNAME": "name",
        "LAND1": "country",
    }
    bldg_table_name = "ZTXXETBLDG"
    bldg_select_fields = "GEBNR,ZBLDGNAME,LAND1"
    bldg_where_field = "endda eq '99991231'"
    return sap_post_req(
        bldg_table_name, bldg_select_fields, bldg_where_field, header_mapping, local
    )


def get_sites(local=False):
    site_table_name = "T001W"
    site_select_fields = "WERKS"
    site_where_field = "ZZBLDG_REF EQ 'X'"
    site_rows = sap_post_req(
        site_table_name, site_select_fields, site_where_field, None, local
    )
    return [site_row["WERKS"] for site_row in site_rows]


def lambda_handler(event, context):
    """Location - Buildings"""

    # get environment variables
    bucket_name = os.environ.get("BUCKET")
    bucket_prefix = os.environ.get("BUILDING_NEW")

    # get the list of Buildings from  ZTXXETBLDG
    bldg_rows = get_buildings(event == "")

    # get all the Site IDs (field WERKS) for active Sites from T001W as a list
    site_id_list = get_sites(event == "")

    # only keep the Buildings that match a Site (which are already selected based on Flag)
    results = []
    for row in bldg_rows:
        # Superior Loc ID is the first 4 digits of the building's Location Reference ID
        site_id = row["id"][:4]

        # if this Site is actively maintained, then append default fields, and add it to results
        if site_id in site_id_list:
            row["Location_Usage_Type"] = "WORK SPACE"
            row["Location_Type_ID"] = "B"
            row["parentid"] = site_id
            row["Time_Profile_ID"] = ""
            row["Default_Currency_ID"] = ""
            results.append(row)

    # manual mode prevents s3 trigger
    try:
        mode = event["mode"]
        file_name = "MANUAL_TESTING_building.json"
    except KeyError:
        file_name = "building.json"

    # write locally and uplaod to s3
    output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"
    write_sap_json(results, output_path)
    upload_file(output_path, bucket_name, bucket_prefix, file_name)

    return {"result_count": len(results)}


# if __name__ == "__main__":
#     with open("environ.json", "r") as f:
#         environ = json.loads(f.read())
#     os.environ["SECRETS"] = environ["SECRETS"]
#     os.environ["BUCKET"] = environ["BUCKET"]
#     os.environ["BUILDING_NEW"] = environ["BUILDING_NEW"]
#     lambda_handler("", "")
