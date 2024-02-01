import pytest
import json
import pycountry
from datetime import date
from process_deltas.helper_soap import (
    add_update_org,
    put_location,
    get_locations,
    put_cost_center,
)
from process_deltas.tests.data import (
    COMPANY_HIERARCHY,
    COMPANY,
    COSTCENTER_HIERARCHY,
    COSTCENTER,
    LOCATION_HIERARCHY,
    SITE,
    BUILDING,
)


def test_addupdateorg_onezeroone():
    data_type = "Company Hierarchy"
    row = COMPANY_HIERARCHY

    # make call to helper_soap
    result, operation = add_update_org(data_type, row)
    assert operation == "Add_Update_Organization"

    # check the dict that represents the soap body
    superior = result["Superior_Organization_Reference"]["ID"]
    assert superior["type"] == "Organization_Reference_ID"
    assert superior["_value_1"] == row["parentid"]
    assert result["Organization_Reference_ID"] == row["id"]
    assert result["Organization_Name"] == row["name"]
    assert result["Availability_Date"] == date.today().isoformat()
    assert result["Include_Organization_Code_In_Name"]
    assert result["Organization_Code"] == row["id"]
    org_type = result["Organization_Type_Reference"]["Organization_Type_Name"]
    assert org_type == data_type
    org_subtype = result["Organization_Subtype_Reference"]["Organization_Subtype_Name"]
    assert org_subtype == "Company"


def test_addupdateorg_companycode():
    data_type = "Company"
    row = COMPANY

    # make call to helper_soap
    result, operation = add_update_org(data_type, row)
    assert operation == "Add_Update_Organization"

    # check the dict that represents the soap body
    container = result["Container_Organization_Reference"]["ID"]
    assert container["type"] == "Organization_Reference_ID"
    assert container["_value_1"] == row["parentid"]
    assert result["Organization_Reference_ID"] == row["id"]
    assert result["Organization_Name"] == row["name"]
    assert result["Availability_Date"] == date.today().isoformat()
    assert result["Include_Organization_Code_In_Name"]
    assert result["Organization_Code"] == row["id"]
    org_type = result["Organization_Type_Reference"]["Organization_Type_Name"]
    org_subtype = result["Organization_Subtype_Reference"]["Organization_Subtype_Name"]
    assert org_type == org_subtype == data_type


def test_addupdateorg_ninetwosix():
    data_type = "Cost Center Hierarchy"
    row = COSTCENTER_HIERARCHY

    # make call to helper_soap
    result, operation = add_update_org(data_type, row)
    assert operation == "Add_Update_Organization"

    # check the dict that represents the soap body
    superior = result["Superior_Organization_Reference"]["ID"]
    assert superior["type"] == "Organization_Reference_ID"
    assert superior["_value_1"] == row["parentid"]
    assert result["Organization_Reference_ID"] == row["id"]
    assert result["Organization_Name"] == row["name"]
    assert result["Availability_Date"] == date.today().isoformat()
    assert result["Include_Organization_Code_In_Name"]
    assert result["Organization_Code"] == row["id"]
    org_type = result["Organization_Type_Reference"]["Organization_Type_Name"]
    assert org_type == data_type
    org_subtype = result["Organization_Subtype_Reference"]["Organization_Subtype_Name"]
    assert org_subtype == "Cost Center"


def test_addupdateorg_costcenter():
    row = COSTCENTER

    # mock get_cost_center to get active status
    row["active_in_wd"] = "false"

    # make call to helper_soap
    result, operation = put_cost_center(row)
    assert operation == "Put_Cost_Center"

    # check the dict that represents the soap body
    assert result == {
        "Restricted_To_Company_or_Company_Hierarchy_Reference": {
            "ID": {"type": "Organization_Reference_ID", "_value_1": row["companycode"]}
        },
        "Organization_Data": {
            "ID": row["id"],
            "Organization_Code": row["id"],
            "Include_Organization_Code_in_Name": "true",
            "Organization_Name": row["name"],
            "Availability_Date": date.today().isoformat(),
            "Organization_Active": row["active_in_wd"],
        },
        "Organization_Subtype_Reference": {
            "ID": {"type": "Organization_Subtype_ID", "_value_1": row["tdcindicator"]}
        },
        "Organization_Container_Reference": {
            "ID": {"type": "Organization_Reference_ID", "_value_1": row["parentid"]}
        },
    }


def test_addupdateorg_ninetwoone():
    data_type = "Location Hierarchy"
    row = LOCATION_HIERARCHY

    # make call to helper_soap
    result, operation = add_update_org(data_type, row)
    assert operation == "Add_Update_Organization"

    # check the dict that represents the soap body
    superior = result["Superior_Organization_Reference"]["ID"]
    assert superior["type"] == "Organization_Reference_ID"
    assert superior["_value_1"] == row["parentid"]
    assert result["Organization_Reference_ID"] == row["id"]
    assert result["Organization_Name"] == row["name"]
    assert result["Availability_Date"] == date.today().isoformat()
    assert result["Include_Organization_Code_In_Name"]
    assert result["Organization_Code"] == row["id"]
    org_type = result["Organization_Type_Reference"]["Organization_Type_Name"]
    assert org_type == data_type
    org_subtype = result["Organization_Subtype_Reference"]["Organization_Subtype_Name"]
    assert org_subtype == "Location"


def test_addupdateorg_error():
    with pytest.raises(Exception) as exc_info:
        row = LOCATION_HIERARCHY
        data_type = "Unknown"
        add_update_org(data_type, row)

    assert str(exc_info.value) == "Unknown data type for add_update_organization"


def test_putlocation_site():
    data_type = "Site"
    row = SITE

    result, operation = put_location(data_type, row)

    assert operation == "Put_Location"
    assert result == {
        "Location_Name": row["name"],
        "Location_ID": row["id"],
        "Location_Usage_Reference": {
            "ID": {"type": "Location_Usage_ID", "_value_1": row["location_usage_type"]}
        },
        "Location_Type_Reference": {
            "ID": {"type": "Location_Type_ID", "_value_1": row["location_type_id"]}
        },
        "Location_Hierarchy_Reference": {
            "ID": {"type": "Organization_Reference_ID", "_value_1": row["parentid"]}
        },
        "Time_Profile_Reference": {
            "ID": {"type": "Time_Profile_ID", "_value_1": row["time_profile_id"]}
        },
        "Display_Language_Reference": {
            "ID": {"type": "User_Language_ID", "_value_1": row["user_language_id"]}
        },
        "Contact_Data": {
            "Address_Data": {
                "Country_Reference": {
                    "ID": {"type": "ISO_3166-1_Alpha-2_Code", "_value_1": row["country"],}
                },
                "Address_Line_Data": [
                    {"Type": "Address_Line_1", "_value_1": row["address_line_1"]},
                    {"Type": "Address_Line_2", "_value_1": row["address_line_2"]},
                ],
                "Municipality": row["municipality"],
                "Country_Region_Reference": {
                    "ID": {
                        "type": "Country_Region_ID",
                        "_value_1": f'{pycountry.countries.get(alpha_2=row["country"]).alpha_3}-{row["region"]}',
                    }
                },
                "Postal_Code": row["postal_code"],
                "Usage_Data": {
                    "Public": "true",
                    "Type_Data": {
                        "Primary": "true",
                        "Type_Reference": {
                            "ID": {"type": "Communication_Usage_Type_ID", "_value_1": "BUSINESS",}
                        },
                    },
                },
                "Submunicipality_Data": [
                    {"Type": "CITY_SUBDIVISION_1", "_value_1": row["city_subdivision_1"],},
                ],
                "Subregion_Data": [
                    {"Type": "REGION_SUBDIVISION_1", "_value_1": row["region_subdivision_1"],},
                ],
            }
        },
    }


def test_putlocation_building():
    data_type = "Building"
    row = BUILDING

    # mocking get_location for building to get active status
    row["inactive_in_wd"] = "true"

    result, operation = put_location(data_type, row)

    assert operation == "Put_Location"
    assert result == {
        "Location_Name": row["name"],
        "Location_ID": row["id"],
        "Inactive": row["inactive_in_wd"],
        "Location_Usage_Reference": {
            "ID": {"type": "Location_Usage_ID", "_value_1": row["location_usage_type"]}
        },
        "Location_Type_Reference": {
            "ID": {"type": "Location_Type_ID", "_value_1": row["location_type_id"]}
        },
        "Superior_Location_Reference": {
            "ID": {"type": "Location_ID", "_value_1": row["parentid"]}
        },
    }


def test_put_location_fail():
    with pytest.raises(Exception) as exc_info:
        data_type = "Unknown"
        row = BUILDING

        put_location(data_type, row)

    assert str(exc_info.value) == "Unknown data type for put_location"


def test_get_location_singular():
    _id = "123456"

    result, operation = get_locations("Site", _id)

    assert operation == "Get_Locations_Site"
    assert result == {
        "Request_References": {
            "Skip_Non_Existing_Instances": "true",
            "Location_Reference": {"ID": {"type": "Location_ID", "_value_1": _id}},
        },
        "Response_Group": {"Include_Reference": "true", "Include_Location_Data": "true",},
        "DNU_id": _id,
    }


def test_location_list():
    _ids = ["1234", "5678", "8910"]

    result, operation = get_locations("Building", _ids)

    assert operation == "Get_Locations_Building"
    assert result == {
        "Request_References": {
            "Skip_Non_Existing_Instances": "true",
            "Location_Reference": [
                {"ID": {"type": "Location_ID", "_value_1": _ids[0]}},
                {"ID": {"type": "Location_ID", "_value_1": _ids[1]}},
                {"ID": {"type": "Location_ID", "_value_1": _ids[2]}},
            ],
        },
        "Response_Group": {"Include_Reference": "true", "Include_Location_Data": "true",},
        "DNU_id": _ids,
    }

