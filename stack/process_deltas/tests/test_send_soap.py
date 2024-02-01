import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3

from process_deltas.tests.data import (
    TENANT,
    COMPANY_HIERARCHY,
    COMPANY,
    COSTCENTER_HIERARCHY,
    COSTCENTER,
    LOCATION_HIERARCHY,
    SITE,
    BUILDING,
    context,
    get_addr_components,
    get_xml,
)

os.environ["NO_FAILS"] = "False"


def test_process_site_valid(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """Valid and Active Site in WD, get the Time Profile from it"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_active.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    site = dict(SITE)
    site["time_profile_id"] = "will be overwritten by what's in WD. blank from athena"
    site["status"] = "A"  # status from g11
    msg = {"body": site}
    event = {"hierarchy": "site", "local": get_addr_components()}

    assert process_site(
        event,
        context,
        msg,
        zeep,
        service,
        version,
        os.environ.get("ERROR_BUCKET"),
        os.environ.get("NO_FAILS"),
    )
    assert msg["body"]["time_profile_id"] == "Standard_Hours_37"


def test_process_site_invalid_wd(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """If the site doesn't exist in workday, sets default time_profile_id"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_invalid.xml"), status_code=500,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    msg = {"body": dict(SITE)}
    event = {"hierarchy": "site", "local": get_addr_components(), "date": "whatever"}

    assert (
        process_site(
            event,
            context,
            msg,
            zeep,
            service,
            version,
            os.environ.get("ERROR_BUCKET"),
            os.environ.get("NO_FAILS"),
        )
        is True
    )
    assert msg["body"]["time_profile_id"] == "Standard_Hours_40"


def test_process_site_invalid_field_nofails(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """Missing a required field, fails validation, write to S3 and but does not throw exception"""
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    error_bucket = os.environ.get("ERROR_BUCKET")
    site = dict(SITE)
    site["postal_code"] = ""
    msg = {"body": site}
    event = {"hierarchy": "site", "local": get_addr_components(), "date": "whatever"}

    with pytest.raises(Exception) as exc_info:
        process_site(
            event, context, msg, zeep, service, version, error_bucket, "False",
        )

    assert (
        str(exc_info.value)
        == "error while validating site. logged in s3: Missing postal_code for Site: 0000"
    )

    client = boto3.client("s3")
    objects = client.list_objects(Bucket=os.environ.get("ERROR_BUCKET"))
    assert msg["body"]["id"] in objects["Contents"][0]["Key"]


def test_process_site_invalid_field_nofails(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """Missing a required field, fails validation, write to S3 and throw exception, won't even make web service for get"""
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    error_bucket = os.environ.get("ERROR_BUCKET")
    site = dict(SITE)
    site["postal_code"] = ""
    msg = {"body": site}
    event = {"hierarchy": "site", "local": get_addr_components(), "date": "whatever"}

    processed = process_site(
        event, context, msg, zeep, service, version, error_bucket, "True",
    )
    assert processed == False

    client = boto3.client("s3")
    objects = client.list_objects(Bucket=os.environ.get("ERROR_BUCKET"))
    assert msg["body"]["id"] in objects["Contents"][0]["Key"]


def test_process_site_inactive_wd(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """Site is inactive in WD, skip"""
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("get_locations_inactive.xml"), status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    error_bucket = os.environ.get("ERROR_BUCKET")
    msg = {"body": dict(SITE)}
    event = {"hierarchy": "site", "local": get_addr_components(), "date": "whatever"}

    assert (
        process_site(
            event,
            context,
            msg,
            zeep,
            service,
            version,
            error_bucket,
            os.environ.get("NO_FAILS"),
        )
        is False
    )


def test_process_site_inactive_g11(
    aws_credentials, secrets_manager, requests_mock, s3_error_bucket
):
    """process_site should return False because site is not active in G11"""
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap import process_site

    zeep, service, version = get_client("Testing")
    zeep.wsse = False

    error_bucket = os.environ.get("ERROR_BUCKET")
    site = dict(SITE)

    # set status to something besides "A"
    site["status"] = "C"
    msg = {"body": site}
    event = {"hierarchy": "site", "local": get_addr_components(), "date": "whatever"}

    assert (
        process_site(
            event,
            context,
            msg,
            zeep,
            service,
            version,
            error_bucket,
            os.environ.get("NO_FAILS"),
        )
        is False
    )


def test_lambda_handler_site_inactive_g11(
    aws_credentials, sqs_fifo, requests_mock, secrets_manager, s3_error_bucket
):
    """Inactive sites will just be deleted from SQS and skipped, no error should be thrown"""
    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_boto3 import sqs_send_message_batch
    from process_deltas import send_soap
    from process_deltas.send_soap import lambda_handler

    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing"
    )
    send_soap.zeep_client.wsse = False

    # write some messages to SQS
    client = boto3.client("sqs")
    sqs_url = os.environ.get("SITE_SQS_URL")
    batch = []
    site = dict(SITE)
    site["status"] = "C"
    for _ in range(3):
        batch.append(site)
    sqs_send_message_batch(client, sqs_url, "1234", batch)

    event = {
        "hierarchy": "site",
        "date": "whatever",
        "deltas_length": 30,
        "mode": "continue",
        "local": get_addr_components(),
    }

    response = lambda_handler(event, "")

    assert response["queue_length"] == 0


def test_lambda_handler_success(
    aws_credentials, sqs_fifo, requests_mock, secrets_manager
):
    """Successfully process/send web service to WD. Delete from SQS"""
    requests_mock.register_uri(
        "POST", TENANT, text="foobar", status_code=200,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_boto3 import sqs_send_message_batch
    from process_deltas import send_soap
    from process_deltas.send_soap import lambda_handler

    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing"
    )
    send_soap.zeep_client.wsse = False

    # write some messages to SQS
    client = boto3.client("sqs")
    sqs_url = os.environ.get("COMPANYCODE_SQS_URL")
    batch = []
    for _ in range(3):
        batch.append(dict(COMPANY))
    sqs_send_message_batch(client, sqs_url, "1234", batch)

    event = {
        "hierarchy": "companycode",
        "date": "whatever",
        "deltas_length": 30,
        "mode": "continue",
    }

    response = lambda_handler(event, "")

    assert response["queue_length"] == 0


def test_lambda_handler_failure_hierarchy(
    aws_credentials, sqs_fifo, requests_mock, secrets_manager, s3_error_bucket
):
    """
    Note: This can't be tested with site or building beacuse they need 
    get_locations AND put_locations to be mocked.
    """

    # mock error response
    requests_mock.register_uri(
        "POST", TENANT, text=get_xml("put_location_invalid.xml"), status_code=500,
    )

    from process_deltas.helper_zeep import get_client
    from process_deltas.helper_boto3 import sqs_send_message_batch, sqs_get_queue_length
    from process_deltas import send_soap
    from process_deltas.send_soap import lambda_handler

    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing"
    )
    send_soap.zeep_client.wsse = False

    # write some messages to SQS
    client = boto3.client("sqs")
    sqs_url = os.environ.get("NINETWOONE_SQS_URL")
    batch = []
    for _ in range(3):
        batch.append(LOCATION_HIERARCHY)
    sqs_send_message_batch(client, sqs_url, "1234", batch)

    event = {
        "hierarchy": "ninetwoone",
        "date": "whatever",
        "deltas_length": 30,
        "mode": "continue",
    }

    with pytest.raises(Exception) as exc_info:
        lambda_handler(event, context)

    assert str(exc_info.value) == "error in hierarchy. logged in s3"
    assert sqs_get_queue_length(client, sqs_url, "inflight") == 3

    client = boto3.client("s3")
    objects = client.list_objects(Bucket=os.environ.get("ERROR_BUCKET"))
    assert "whatever_ninetwoone_0000" in objects["Contents"][0]["Key"]
