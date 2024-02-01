from datetime import date

import pycountry


def get_country_region_id(iso2_country: str, region: str) -> str:
    """get iso3 digit country id and append to region to form country region id"""

    # get the the iso3 from iso2 stored in sap
    search = pycountry.countries.search_fuzzy(iso2_country)[0]
    iso3_country = search.alpha_3

    return f"{iso3_country}-{region}".upper()


def add_update_org(hierarchy, msg):
    """
    Set up the dict to use in Add_Update_Organization SOAP call, returns a dict and the zeep operation to use
    """

    fields = {}
    parent = {}

    org_type_name = hierarchy

    if msg["parentid"]:
        parent["ID"] = {
            "type": "Organization_Reference_ID",
            "_value_1": f"{msg['parentid']}",
        }
        if hierarchy == "Company":
            fields["Container_Organization_Reference"] = parent
        else:
            fields["Superior_Organization_Reference"] = parent

    # default value for including code in name is false - only modify if required in specific hierarchy/data type
    fields["Include_Organization_Code_In_Name"] = "false"

    # org subtype is slightly different for each hierarchy
    if hierarchy == "Company":
        org_subtype_name = hierarchy
        fields["Include_Organization_Code_In_Name"] = "true"
    elif hierarchy == "Cost Center Hierarchy":
        org_subtype_name = "Cost Center"
    elif hierarchy == "Location Hierarchy":
        org_subtype_name = "Location"
    elif hierarchy == "Company Hierarchy":
        org_subtype_name = "Company"
    else:
        raise Exception("Unknown data type for add_update_organization")

    # these fields are the same bewteen calls
    fields["Organization_Reference_ID"] = msg["id"]
    fields["Organization_Name"] = msg["name"]
    fields["Availability_Date"] = date.today().isoformat()
    fields["Organization_Code"] = msg["id"]
    fields["Organization_Type_Reference"] = {"Organization_Type_Name": org_type_name}
    fields["Organization_Subtype_Reference"] = {"Organization_Subtype_Name": org_subtype_name}
    fields["Organization_Visibility_Reference"] = {"Organization_Visibility_Name": "Everyone"}

    return fields, "Add_Update_Organization"


def put_cost_center(msg):
    """
    Put Cost Center comes from Financial Management, this is because we need to restrict Cost Centers to a company
    
    Note: Put_Cost_Center defaults status to Inactive
    """
    fields = {}

    # Restrict cost center to company code if it exits
    if msg["companycode"]:
        fields["Restricted_To_Company_or_Company_Hierarchy_Reference"] = {
            "ID": {"type": "Organization_Reference_ID", "_value_1": msg["companycode"]}
        }

    # Organzation Data
    org_data = fields["Organization_Data"] = {}
    org_data["ID"] = msg["id"]
    org_data["Organization_Code"] = msg["id"]
    org_data["Include_Organization_Code_in_Name"] = "true"
    org_data["Organization_Name"] = msg["name"]
    org_data["Availability_Date"] = date.today().isoformat()

    # this comes grom get_cost_center
    org_data["Organization_Active"] = msg["active_in_wd"]

    # Organization Visibility Reference inherited from its top-level org. If not avaulable, default so Everyone

    # Other fields
    fields["Organization_Subtype_Reference"] = {
        "ID": {"type": "Organization_Subtype_ID", "_value_1": msg["tdcindicator"]}
    }
    fields["Organization_Container_Reference"] = {
        "ID": {"type": "Organization_Reference_ID", "_value_1": msg["parentid"]}
    }

    return fields, "Put_Cost_Center"


def put_location(operation, msg):
    """
    Set up the dict to use in the Put_Location SOAP call, returns a dict and the zeep operation to use.
    
    Note: Put_Location default status to Active
    """

    fields = {}
    fields["Location_Name"] = msg["name"]
    fields["Location_ID"] = msg["id"]

    # this will default to active on creating
    # fields["Inactive"] = "false"

    # call for building is simple, no need for address data
    if operation == "Building":
        fields["Inactive"] = msg["inactive_in_wd"]

        # this defaults to BUILDING SITE
        fields["Location_Usage_Reference"] = {
            "ID": {"type": "Location_Usage_ID", "_value_1": f"{msg['location_usage_type']}",}
        }
        fields["Location_Type_Reference"] = {
            "ID": {"type": "Location_Type_ID", "_value_1": f"{msg['location_type_id']}",}
        }

        # we use superior when the parent is another location of the same object type (buildings -> sites)
        fields["Superior_Location_Reference"] = {
            "ID": {"type": "Location_ID", "_value_1": f"{msg['parentid']}",}
        }

    # sites is complex and comes with address data
    elif operation == "Site":
        fields["Location_Usage_Reference"] = {
            "ID": {"type": "Location_Usage_ID", "_value_1": f"{msg['location_usage_type']}",}
        }
        fields["Location_Type_Reference"] = {
            "ID": {"type": "Location_Type_ID", "_value_1": f"{msg['location_type_id']}",}
        }

        # we use location hierarchy reference to point to hierarchy, which "contains" all the locations
        fields["Location_Hierarchy_Reference"] = {
            "ID": {"type": "Organization_Reference_ID", "_value_1": f"{msg['parentid']}",}
        }

        # time profile reference is required for business site
        fields["Time_Profile_Reference"] = {
            "ID": {"type": "Time_Profile_ID", "_value_1": f"{msg['time_profile_id']}"}
        }

        # likely don't need
        # fields["Locale_Reference"] = {"ID": {"type": "Locale_ID", "_value_1": "en_US"}}

        fields["Display_Language_Reference"] = {
            "ID": {"type": "User_Language_ID", "_value_1": f"{msg['user_language_id']}",}
        }

        # 06/23/20 - we will remove currency - it is not needed and will automatically be set by WD based on country
        # fields["Default_Currency_Reference"] = {
        #     "ID": {
        #         "type": "Currency_ID",
        #         "_value_1": f"{msg['default_currency_id']}",
        #     }
        # }

        # CONTACT DATA
        address_data = {}
        address_data["Country_Reference"] = {
            "ID": {"type": "ISO_3166-1_Alpha-2_Code", "_value_1": f"{msg['country']}",}
        }

        address_data["Address_Line_Data"] = []

        if msg["address_line_1"]:
            address_data["Address_Line_Data"].append(
                {"Type": "Address_Line_1", "_value_1": f"{msg['address_line_1']}"}
            )

        if msg["address_line_2"]:
            address_data["Address_Line_Data"].append(
                {"Type": "Address_Line_2", "_value_1": f"{msg['address_line_2']}"}
            )

        # can be blank if it doesn't have one
        if msg["municipality"]:
            address_data["Municipality"] = f"{msg['municipality']}"

        # need to skip this entire block if there is no Country_Region_Reference
        if msg["region"]:
            # address_data["Country_Region_Reference"] = {
            #     "ID": {"type": "ISO_3166-2_Code", "_value_1": f"{msg['region']}",}
            # }
            address_data["Country_Region_Reference"] = {
                "ID": {
                    "type": "Country_Region_ID",
                    "_value_1": get_country_region_id(msg["country"], msg["region"]),
                }
            }

        if msg["postal_code"]:
            address_data["Postal_Code"] = f"{msg['postal_code']}"

        # Required - Usgae Data is required unless address is being deleted
        address_data["Usage_Data"] = {
            "Public": "true",
            "Type_Data": {
                "Primary": "true",
                "Type_Reference": {
                    "ID": {"type": "Communication_Usage_Type_ID", "_value_1": "BUSINESS",}
                },
            },
        }

        # these are edge cases, most of them should be blank
        if msg["city_subdivision_1"]:
            address_data["Submunicipality_Data"] = []

            # the address component name should resolve automatically
            address_data["Submunicipality_Data"].append(
                {
                    "Type": "CITY_SUBDIVISION_1",
                    # "Address_Component_Name": "Postal District or City",
                    "_value_1": f"{msg['city_subdivision_1']}",
                }
            )

        if msg["region_subdivision_1"]:
            address_data["Subregion_Data"] = []

            # the address component name should resolve automatically
            address_data["Subregion_Data"].append(
                {"Type": "REGION_SUBDIVISION_1", "_value_1": f"{msg['region_subdivision_1']}",}
            )

        # add all the address data to the rest of the SOAP call
        fields["Contact_Data"] = {"Address_Data": address_data}

    else:
        raise Exception("Unknown data type for put_location")

    return fields, "Put_Location"


def get_locations(location_type, location_ids):
    """This creates a SOAP call to get either 1, N, or all locations. Paging not implemented so max results is 10"""
    fields = {}

    if isinstance(location_ids, list):
        # Can do bulk query of multiple location ids, but whole thing returns error if any invalid
        location_references = []
        for loc_id in location_ids:
            location_references.append({"ID": {"type": "Location_ID", "_value_1": f"{loc_id}"}},)
        fields["Request_References"] = {
            "Skip_Non_Existing_Instances": "true",
            "Location_Reference": location_references,
        }
    else:
        # or lookup a single location id
        if location_ids:
            fields["Request_References"] = {
                "Skip_Non_Existing_Instances": "true",
                "Location_Reference": {
                    "ID": {"type": "Location_ID", "_value_1": f"{location_ids}"}
                },
            }

    # otherwise we get ALL locations
    fields["Response_Group"] = {
        "Include_Reference": "true",
        "Include_Location_Data": "true",
    }

    # since the response doesn't include the id
    fields["DNU_id"] = location_ids

    # need to differentiate between site and building in response (to get time profile or not)
    return fields, f"Get_Locations_{location_type}"


def get_cost_center(costcenter_id):
    """This will only create the SOAP envelope to get one cost center"""
    fields = {}

    if costcenter_id:
        fields["Request_References"] = {
            "Cost_Center_Reference": {
                "ID": {"type": "Organization_Reference_ID", "_value_1": f"{costcenter_id}",}
            },
        }

    fields["Response_Group"] = {
        "Include_Reference": "false",
        "Include_Cost_Center_Data": "true",
        "Include_Simple_Cost_Center_Data": "false",
    }

    return fields, "Get_Cost_Centers"
