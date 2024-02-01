import pytest
import json
import os
from process_deltas.helper_validation import (
    manual_fixes,
    validate_fields,
    validate_site,
)
from process_deltas.tests.data import SITE


def get_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "addr_components.json")


def get_file():
    with open(get_path(), "r") as f:
        file = f.read()
    return json.loads(file)


def get_countries(filter="all"):
    """
    Return a list of countries from the address component document. Set filter to a field
    to get a list of countries that require that field
    """
    temp = set()

    if filter == "all":
        for country in get_file().keys():
            temp.add(country)
    elif filter == "manual_fixes":
        for key, value in get_file().items():
            if len(value["fixes"]) > 0:
                temp.add(key)
    else:
        for key, value in get_file().items():
            if filter in value["required"]:
                temp.add(key)

    countries = []
    for country in temp:
        countries.append((country))

    return countries


def get_data(country, skip=None):
    """Return a dict that represents a row for site coming out of Athena"""
    data = dict(SITE)
    data["country"] = country
    # can remove certain fields
    if skip:
        data[skip] = ""
    return data


def check_non_maintained_fields(data):
    assert data["id"]
    assert data["name"]
    assert data["status"]
    assert data["location_type_id"]
    assert data["country"]
    assert data["user_language_id"]
    assert data["location_usage_type"]
    assert data["parentid"]
    assert data["default_currency_id"]
    assert data["time_profile_id"]


"""THESE ARE ALL LISTS OF COUNTRIES THAT REQUIRED SAID FIELDS"""


@pytest.fixture(scope="module")
def get_addr_components():
    return get_file()


@pytest.fixture(scope="module")
def addr_line_1():
    return get_countries("Address Line 1")


@pytest.fixture(scope="module")
def addr_line_2():
    return get_countries("Address Line 2")


@pytest.fixture(scope="module")
def postal_code():
    return get_countries("Postal Code")


@pytest.fixture(scope="module")
def municipality():
    return get_countries("City")


@pytest.fixture(scope="module")
def region():
    return get_countries("Region")


@pytest.fixture(scope="module")
def city_subdivision():
    return get_countries("City Subdivision 1")


@pytest.fixture(scope="module")
def region_subdivision():
    return get_countries("Region Subdivision 1")


"""TEST FIELD VALIDATION"""


@pytest.mark.parametrize("country", get_countries())
def test_validate_fields_all(
    get_addr_components,
    addr_line_1,
    addr_line_2,
    postal_code,
    municipality,
    region,
    region_subdivision,
    city_subdivision,
    country,  # parametrize on all countries
):
    """Check that we are able to remove all except required fields"""
    validations = get_addr_components
    data = get_data(country)

    # check successful validation
    assert validate_fields(data, validations[country]["required"]) is True

    # check that non-maintained fields were not removed
    check_non_maintained_fields(data)

    # check that fields that were not required are removed
    if country not in addr_line_1:
        assert data["address_line_1"] == ""
    if country not in addr_line_2:
        assert data["address_line_2"] == ""
    if country not in postal_code:
        assert data["postal_code"] == ""
    if country not in municipality:
        assert data["municipality"] == ""
    if country not in region:
        assert data["region"] == ""
    if country not in city_subdivision:
        assert data["city_subdivision_1"] == ""
    if country not in region_subdivision:
        assert data["region_subdivision_1"] == ""


@pytest.mark.parametrize("country", get_countries("Address Line 1")[:10])
def test_validate_fields_addr1(get_addr_components, country):
    validations = get_addr_components
    data = get_data(country, "address_line_1")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing address_line_1 for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("Address Line 2")[:10])
def test_validate_fields_addr2(get_addr_components, country):
    validations = get_addr_components
    data = get_data(country, "address_line_2")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing address_line_2 for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("City")[:10])
def test_validate_fields_municipality(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country, "municipality")
    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing municipality for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("Postal Code")[:10])
def test_validate_fields_postalcode(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country, "postal_code")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing postal_code for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("Region")[:10])
def test_validate_fields_region(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country, "region")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing region for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("City Subdivision 1")[:10])
def test_validate_fields_citysubdivision1(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country, "city_subdivision_1")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing city_subdivision_1 for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("Region Subdivision 1")[:10])
def test_validate_fields_regionsubdivision1(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country, "region_subdivision_1")

    error_msg = validate_fields(data, validations[country]["required"])
    assert error_msg == f"Missing region_subdivision_1 for Site: {data['id']}"


@pytest.mark.parametrize("country", get_countries("manual_fixes"))
def test_manual_fixes(
    get_addr_components, country,  # parametrize on all countries
):
    validations = get_addr_components
    data = get_data(country)

    manual_fixes(data, validations[country]["fixes"])


def test_manual_fixes_invalid(get_addr_components):
    """test an invalid fix, should gracefully skip"""

    validations = get_addr_components

    country = "MYCOUNTRY"
    data = get_data(country)

    # add an invalid fix
    validations[country] = {"fixes": ["not a real fix"]}

    manual_fixes(data, validations[country]["fixes"])

    # if invaid fix, they are not changed and no error is thrown
    assert data == get_data(country)


"""TESTING EVERYTHING TOGETHER"""


@pytest.mark.parametrize("country", get_countries())
def test_validate_site(
    addr_line_1,
    addr_line_2,
    postal_code,
    municipality,
    region,
    region_subdivision,
    city_subdivision,
    country,  # parametrize on all countries
):
    """Check that we are able to remove all except required fields"""
    data = get_data(country)

    # check successful validation
    assert validate_site(get_path(), data) is True

    # check that non-maintained fields were not removed
    check_non_maintained_fields(data)

    # check that fields that were not required are removed
    if country not in addr_line_1:
        assert data["address_line_1"] == ""
    if country not in addr_line_2:
        assert data["address_line_2"] == ""
    if country not in postal_code:
        assert data["postal_code"] == ""
    if country not in municipality:
        assert data["municipality"] == ""
    if country not in region:
        assert data["region"] == ""
    if country not in city_subdivision:
        assert data["city_subdivision_1"] == ""
    if country not in region_subdivision:
        assert data["region_subdivision_1"] == ""


def test_validate_site_pr():
    """EDGE CASE: Puerto Rico is an considered a USA state"""
    data = get_data("PR")

    assert validate_site(get_path(), data) is True

    assert data["country"] == "US"
    assert data["region"] == "PR"
    assert data["address_line_1"]
    assert data["postal_code"]
    assert data["municipality"]
    assert not data["city_subdivision_1"]
    assert not data["region_subdivision_1"]
    assert not data["address_line_2"]


def test_validate_site_sk():
    """EDGE CASE: Slovakia requires city -> city_subdivision_1"""
    data = get_data("SK")
    city = data["municipality"]

    assert validate_site(get_path(), data) is True
    assert data["country"] == "SK"

    # check that fix was applied
    assert not data["municipality"]  # removes city
    assert data["city_subdivision_1"] == city  # put in city_subdivision_1


def test_validate_site_fi():
    """EDGE CASE """
    data = get_data("FI")
    city = data["municipality"]  # get the municipality

    assert validate_site(get_path(), data) is True
    assert data["country"] == "FI"

    # check that fix was applied
    assert data["city_subdivision_1"] == city  # put in city_subdivision_1

    # non-required fields removed, only required ones left
    assert not data["municipality"]  # removes city
    assert not data["region"]
    assert data["address_line_1"]
    assert data["postal_code"]


def test_validate_site_hu():
    """EDGE CASE: Hungary is the only country which requires moving address_line_1 -> address_line_2"""
    data = get_data("HU")
    address_line_1 = data["address_line_1"]

    assert validate_site(get_path(), data) is True
    assert data["country"] == "HU"

    # check that address is correctly moved
    assert not data["address_line_1"]
    assert data["address_line_2"] == address_line_1

    # non-required fields removed, only required ones left
    assert data["municipality"]
    assert data["postal_code"]


def test_validate_site_rw():
    """EDGE CASE: Rwanda requires region_subdivision_2?"""
    data = get_data("RW")
    region_1 = data["region_subdivision_1"] = "RW_REGION_SUBDIVISION_1"
    region_2 = data["region_subdivision_2"] = "RW_REGION_SUBDIVISION_2"

    assert validate_site(get_path(), data) is True

    assert data["region_subdivision_1"] == region_1
    assert data["region_subdivision_2"] == region_2
