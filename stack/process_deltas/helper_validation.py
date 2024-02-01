import json
import os


def manual_fixes(row, fixes):
    """process hardcoded fixes from the validation file. these represent manual efforts"""
    try:
        if "copy address line 1 into address line 2" in fixes:
            # HU
            row["address_line_2"] = row["address_line_1"]

        # these are still required
        if "add city into region subdivision 1" in fixes:
            # EE
            row["region_subdivision_1"] = row["municipality"]
        if "add city into city subdivision 1" in fixes:
            # SK, FI
            row["city_subdivision_1"] = row["municipality"]

        # puerto rico edge case
        if "set country to 'US'" in fixes:
            row["country"] = "US"
        if "set region to 'PR'" in fixes:
            row["region"] = "PR"

        # removals come in the end - may not be needed
        if "remove address line 1" in fixes:
            row["address_line_1"] = ""
        if "remove region" in fixes:
            row["region"] = ""
        if "remove city" in fixes:
            row["municipality"] = ""
        if "remove postal code" in fixes:
            row["postal_code"] = ""
    except KeyError:
        pass


def validate_fields(row, required_fields):
    """
    Processes the address fields for this site:
        1. Check that required fields for this site's country are populated
        2. Remove extraneous fields that are not required
    
    Returns True if all validations passed/fixed, or the error message otherwise
    """
    # mapping what we are naming the fields to what they are in the validation file
    maintained_fields = {
        "address_line_1": "Address Line 1",
        "address_line_2": "Address Line 2",
        "municipality": "City",
        "postal_code": "Postal Code",
        "region": "Region",
        "city_subdivision_1": "City Subdivision 1",
        "region_subdivision_1": "Region Subdivision 1",
    }

    error_msg = ""

    # get the site_id for the current row
    site_id = row["id"]

    # loop through all of the fields that we are maintaining, and check if required in validations
    for soap_field, validation_field in maintained_fields.items():

        # if the field is required and we didn't populate it from G11, this row is error because required field is missing
        if validation_field in required_fields and row[soap_field] == "":
            error_msg = f"Missing {soap_field} for Site: {site_id}"
            break

        # if the field is not required, remove it, and continue
        elif validation_field not in required_fields and row[soap_field] != "":
            row[soap_field] = ""

    else:
        # Add to results if all validations successful
        return True

    # if we get here, then something failed and we return the error
    return error_msg


def validate_site(validation_file, row):
    """
    Run manual fixes and address validations on data if it's a Site. Validations are maintained
    outside of Workday in a local file in this repo: addr_components.json
    
    Process:
        1. Apply manual fixes e.g. Move Addr Line 1 -> Addr Line 2
        2. Check if all the required address fields exist
        3. Remove non-required address fields
    
    Returns True if passes all validations, otherwise returns the error message
    """

    # Load validations file
    addr_components = validation_file
    with open(addr_components) as f:
        validations = json.loads(f.read())

    country_iso_2 = row["country"]
    country_info = validations[country_iso_2]
    fixes = validations[country_iso_2]["fixes"]
    required_fields = country_info["required"]

    # run manual fixes - these are things that can't be fixed in G11
    manual_fixes(row, fixes)

    # validate the fields, and return success / error msg
    return validate_fields(row, required_fields)
