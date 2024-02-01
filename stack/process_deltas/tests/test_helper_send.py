import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3
from process_deltas.tests.data import (
    TENANT,
    TENANT_FM,
    COMPANY_HIERARCHY,
    COMPANY,
    COSTCENTER_HIERARCHY,
    COSTCENTER,
    LOCATION_HIERARCHY,
    SITE,
    BUILDING,
    context,
    get_xml,
)


def test_write_error_s3(aws_credentials, s3_error_bucket):
    from process_deltas.helper_send import write_error_s3

    conn = boto3.client("s3")
    error_bucket = os.environ.get("ERROR_BUCKET")

    event = {"hierarchy": "onezeroone", "date": "2020-07-12", "queue_length": "400"}
    msg = {"body": {"id": "1234"}}
    response = "Invalid Response"

    write_error_s3(error_bucket, event, context, msg, response)

    objects = conn.list_objects(Bucket=error_bucket)

    assert (
        "2020-07-12/onezeroone/2020-07-12_onezeroone_1234.txt"
        in objects["Contents"][0]["Key"]
    )


def test_send_web_service_invalid(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    with pytest.raises(Exception) as exc_info:
        send_web_service("invalid", COMPANY_HIERARCHY, zeep, service, version)

    assert str(exc_info.value) == "Unknown web service"


def test_send_web_service_onezeroone(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    assert send_web_service("onezeroone", COMPANY_HIERARCHY, zeep, service, version)


def test_send_web_service_companycode(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    assert send_web_service("companycode", COMPANY, zeep, service, version)


def test_send_web_service_ninetwosix(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    assert send_web_service("ninetwosix", COSTCENTER_HIERARCHY, zeep, service, version)


def test_send_web_service_costcenter(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("put_cost_center_valid.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    row = COSTCENTER
    row["active_in_wd"] = "true"
    assert send_web_service("costcenter", row, zeep, service, version)


def test_send_web_service_ninetwoone(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    assert send_web_service("ninetwoone", LOCATION_HIERARCHY, zeep, service, version)


def test_send_web_service_site(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("put_location_valid.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    assert send_web_service("site", SITE, zeep, service, version)


def test_send_web_service_building(aws_credentials, secrets_manager, requests_mock):
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("put_location_valid.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    row = BUILDING
    # mock a get_location
    row["inactive_in_wd"] = "true"
    assert send_web_service("building", row, zeep, service, version)


def test_send_web_service_getinactivesite(
    aws_credentials, secrets_manager, requests_mock
):
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_active.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    # site is not inactive, returns the time_profile
    time_profile = send_web_service("GET_INACTIVE_SITE", "0009", zeep, service, version)
    assert time_profile == "Standard_Hours_37"


def test_send_web_service_getinactivebuilding(
    aws_credentials, secrets_manager, requests_mock
):
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_active.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_send import send_web_service

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    # building is not inactive
    assert not send_web_service("GET_INACTIVE_BUILDING", "0009", zeep, service, version)
