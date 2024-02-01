import pytest
import json
import os
import requests

from mock import patch

from process_deltas.tests.data import (
    SITE,
    LOCATION_HIERARCHY,
    COSTCENTER,
    TENANT,
    TENANT_FM,
    get_xml,
)


def test_get_client(secrets_manager):
    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_soap import get_locations, put_location, add_update_org

    zeep, service, version = get_client("Testing")
    assert zeep
    assert zeep.wsse
    assert service
    assert version


def test_get_client_wsse(secrets_manager):
    """Test that the patches we made to Zeep for WSSE header are correct"""

    from lxml import etree
    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_soap import get_locations, put_location, add_update_org

    zeep, service, version = get_client("Testing")

    payload, request = put_location("Site", msg=SITE)

    node = zeep.create_message(
        service, request, Location_Data=payload, version=version,
    )
    tree = etree.tostring(node).decode()

    assert "<SignatureValue>" in tree and "</SignatureValue>" in tree
    assert "<X509Certificate>" in tree and "</X509Certificate>" in tree
    assert "<wsse:SecurityTokenReference>" not in tree
    assert "</wsse:SecurityTokenReference>" not in tree
    assert '<Reference URI="">' in tree

    assert '<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>' in tree
    assert (
        '<Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>' not in tree
    )
    assert (
        '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
        in tree
    )
    assert (
        '<SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
        in tree
    )


def test_send_request_get_location_site_active(secrets_manager, requests_mock):
    """Get Locations for an active site in WD, should retrieve the time profile for the site in WD"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_active.xml"), status_code=200,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Site", "0009")
    time_profile = send_request(request, zeep, service, version, payload)

    assert time_profile == "Standard_Hours_37"


def test_send_request_get_location_inactive(secrets_manager, requests_mock):
    """Get Locations for inactive site: Will not get any time profile. response is false and skip this site"""

    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_inactive.xml"), status_code=200,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Site", "0009")
    time_profile = send_request(request, zeep, service, version, payload)

    assert time_profile is None


def test_send_request_get_location_site_invalid(secrets_manager, requests_mock):
    """Site doesn't exits in Workday, default to 40 hours time profile"""

    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_invalid.xml"), status_code=400,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Site", "0009")
    response = send_request(request, zeep, service, version, payload)

    assert response == "Standard_Hours_40"


def test_send_request_put_location_site_valid(secrets_manager, requests_mock):
    """Returns true when the site exits in Workday"""

    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("put_location_valid.xml"), status_code=200,
    )

    from process_deltas.helper_soap import put_location
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = put_location("Site", msg=SITE)
    response = send_request(request, zeep, service, version, payload)

    assert response


def test_send_request_put_location_invalid(secrets_manager, requests_mock):
    """Invalid Location ID when sending a site to WD"""

    invalid = """<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Body>
        <SOAP-ENV:Fault xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" 
            xmlns:wd="urn:com.workday/bsvc">
            <faultcode>SOAP-ENV:Client.validationError</faultcode>
            <faultstring>Validation error occurred. Invalid ID value.  '45873' is not a valid ID value for type = 'Location_ID'</faultstring>
            <detail>
                <wd:Validation_Fault>
                    <wd:Detail_Message>Invalid ID value.  '45873' is not a valid ID value for type = 'Location_ID'</wd:Detail_Message>
                </wd:Validation_Fault>
            </detail>
        </SOAP-ENV:Fault>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".strip()

    requests_mock.register_uri(
        "POST", TENANT, text=invalid, status_code=400,
    )

    from process_deltas.helper_soap import put_location
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = put_location("Site", msg=SITE)

    response = send_request(request, zeep, service, version, payload)

    assert (
        "Validation error occurred. Invalid ID value." in response
        and "45873" in response
    )


def test_send_request_get_location_building_active(secrets_manager, requests_mock):
    """Get Locations for an active building in WD, return whether building is inactive"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_active.xml"), status_code=200,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Building", "0009")
    inactive = send_request(request, zeep, service, version, payload)

    assert inactive is False


def test_send_request_get_location_building_inactive(secrets_manager, requests_mock):
    """Get Locations for an inactive building in WD, return whether building is inactive"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_inactive.xml"), status_code=200,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Building", "0009")
    inactive = send_request(request, zeep, service, version, payload)

    assert inactive is True


def test_send_request_get_location_building_invalid(secrets_manager, requests_mock):
    """Get Locations for an building which doesn't exist in WD, return inactive=False to add building to WD"""

    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_invalid.xml"), status_code=400,
    )

    from process_deltas.helper_soap import get_locations
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = get_locations("Building", "0009")
    inactive = send_request(request, zeep, service, version, payload)

    assert inactive is False


def test_send_request_add_update_org_valid(secrets_manager, requests_mock):
    """Receive valid response when sending Location Hierarchy to WD"""

    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("put_location_valid.xml"), status_code=200,
    )

    from process_deltas.helper_soap import add_update_org
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = add_update_org("Location Hierarchy", msg=LOCATION_HIERARCHY)

    response = send_request(request, zeep, service, version, payload)

    assert response


def test_send_request_add_update_org_invalid(secrets_manager, requests_mock):
    """Invalid Organization ID when sending a Location Hierarchy to WD"""
    invalid = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Body>
        <SOAP-ENV:Fault xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wd="urn:com.workday/bsvc">
            <faultcode>SOAP-ENV:Client.validationError</faultcode>
            <faultstring>Validation error occurred. Invalid ID value.  '4321' is not a valid ID value for type = 'Organization_Reference_ID'</faultstring>
            <detail>
                <wd:Validation_Fault>
                    <wd:Detail_Message>Invalid ID value.  '4321' is not a valid ID value for type = 'Organization_Reference_ID'</wd:Detail_Message>
                </wd:Validation_Fault>
            </detail>
        </SOAP-ENV:Fault>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".strip()

    requests_mock.register_uri(
        "POST", TENANT, text=invalid, status_code=400,
    )

    from process_deltas.helper_soap import add_update_org
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    payload, request = add_update_org("Location Hierarchy", msg=LOCATION_HIERARCHY)

    response = send_request(request, zeep, service, version, payload)

    assert (
        "Validation error occurred. Invalid ID value." in response
        and "4321" in response
    )


def test_send_request_get_cost_center_valid(secrets_manager, requests_mock):
    """Get Active Status from Cost Center"""

    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("get_cost_center_valid.xml"), status_code=200,
    )

    from process_deltas.helper_soap import get_cost_center
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    payload, request = get_cost_center("5203913200")

    response = send_request(request, zeep, service, version, payload)

    assert response == "true"


def test_send_request_get_cost_center_invalid(secrets_manager, requests_mock):
    """If Cost Center doesn't exist, get_cost_center will return active and we will create active Cost Center in WD"""
    invalid = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Body>
        <SOAP-ENV:Fault xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wd="urn:com.workday/bsvc">
            <faultcode>SOAP-ENV:Client.validationError</faultcode>
            <faultstring>Validation error occurred. Invalid ID value.  '4321' is not a valid ID value for type = 'Organization_Reference_ID'</faultstring>
            <detail>
                <wd:Validation_Fault>
                    <wd:Detail_Message>Invalid ID value.  '4321' is not a valid ID value for type = 'Organization_Reference_ID'</wd:Detail_Message>
                </wd:Validation_Fault>
            </detail>
        </SOAP-ENV:Fault>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".strip()

    requests_mock.register_uri("POST", TENANT_FM, text=invalid, status_code=500)

    from process_deltas.helper_soap import get_cost_center
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    payload, request = get_cost_center("5203913200")
    response = send_request(request, zeep, service, version, payload)

    assert response == "true"


def test_send_request_get_cost_center_error_unhandled(secrets_manager, requests_mock):
    """If unknown error when running Get_Cost_Center (Not invalid cost center error, which just means it doesn't exist), raise Exception"""
    invalid = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Body>
        <SOAP-ENV:Fault xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wd="urn:com.workday/bsvc">
            <faultcode>SOAP-ENV:Client.validationError</faultcode>
            <faultstring>UNKNOWN ERROR</faultstring>
        </SOAP-ENV:Fault>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".strip()

    requests_mock.register_uri("POST", TENANT_FM, text=invalid, status_code=500)

    from process_deltas.helper_soap import get_cost_center
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    payload, request = get_cost_center("5203913200")

    with pytest.raises(Exception) as exc_info:
        send_request(request, zeep, service, version, payload)

    assert str(exc_info.value) == "Unknown error from Get_Cost_Centers: UNKNOWN ERROR"


def test_send_request_put_cost_center_success(secrets_manager, requests_mock):
    """Successfully PUT a Cost Center"""
    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("put_cost_center_valid.xml"), status_code=200
    )

    from process_deltas.helper_soap import put_cost_center
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    row = COSTCENTER
    row["id"] = "007_YB"
    row["active_in_wd"] = "true"
    payload, request = put_cost_center(row)

    response = send_request(request, zeep, service, version, payload)
    assert response is True


def test_send_request_put_cost_center_error(secrets_manager, requests_mock):
    """Error when trying to PUT a Cost Center, get error message"""
    invalid = """<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Body>
        <SOAP-ENV:Fault xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wd="urn:com.workday/bsvc">
            <faultcode>SOAP-ENV:Client.validationError</faultcode>
            <faultstring>UNKNOWN ERROR</faultstring>
        </SOAP-ENV:Fault>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
""".strip()

    requests_mock.register_uri("POST", TENANT_FM, text=invalid, status_code=500)

    from process_deltas.helper_soap import put_cost_center
    from process_deltas.helper_zeep import get_client, send_request

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    row = COSTCENTER
    row["id"] = "007_YB"
    row["active_in_wd"] = "true"
    payload, request = put_cost_center(row)

    response = send_request(request, zeep, service, version, payload)
    assert response == "UNKNOWN ERROR"
