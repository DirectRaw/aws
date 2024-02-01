import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3
from datetime import date

from process_deltas.tests.data import TENANT_FM, COSTCENTER, context, get_xml

# this is what SQS Lambda triggers pass to Lambda in Event
event = {
    "Records": [
        {
            "messageId": "f52682aa-88fd-40c7-8c99-7135e4b0806a",
            "receiptHandle": "AQEBpFzuPiaILzab0dAJRgnxwYVmVCVtZlMJeiYccpJ18t+wdYpv3HSmh44p6eGuazYt/RIPPBQAZo8fZi9BH2mOHKfVtL+rtHVC6OGrJaaxtl+QEYm0ZJ+DVH20p1a4M/bDPYrB8nwQ7TZE3yIqs/YEC6rGDqjnWRLdBaq86MB0CBzZ5IcfHJE7+ye8oamWUgmj9YwlIQqrYxvsh+6P3dwv7he4+k+JZNOINcyThLwirXniHjXoH61Z9p2qlQUk9+c2LuDRjxI2pH3w7f7aE6g7SEKgGnj4xQ5fZQZPe+vYFDJHMswNGZDoLEl50AwJKgFXxtwtFZAkPkOjD/4vFPVsX68oY1qBPtQxedJWMrakdL+EsrLcZ2iHf2zNQNZimbeyzl1uuQh115qelRkzw9qSl8okrAPpHCOfivXMsz1WfDQ=",
            "body": '{"Inactive": "N", "id": "CS00000306", "parentid": "CCSS000305", "name": "NA SMO WIDE", "rootflag": "Y", "lvl": "5", "parentlvl": "4", "parentname": "NA MKT OPS - NA SMO WIDE TOTAL", "tdcindicator": "ABC", "companycode": "123"}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1594830519182",
                "SenderId": "AROA3OSU3UXXLZMDDJCX6:huang.sg.2@pg.com",
                "ApproximateFirstReceiveTimestamp": "1594831566760",
            },
            "messageAttributes": {},
            "md5OfBody": "a09c0f871424153f76ae55645ae09c86",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:787230729710:workday-hierarchies-dev-costcenter-queue",
            "awsRegion": "us-east-1",
        },
        {
            "messageId": "869ed024-0091-4d5b-857b-75caf921b7c5",
            "receiptHandle": "AQEBPyueofuZlhug+izy+3YutQk87DtgzAnC2JsHn17BaurF4HXGXg5150xypfKecxmK+r5Y6/mHP/uT7QR1DQCTi2PPAGlVSmeNF67IwoLUhNeZpnJjDlkuIkuhkO7+1sJPOzJ702BZos8el0zJ+gBFTOUjDdZ46G02PlLPON/a13+XsOXII5Ba62VoXqEbAUzqRdqnA4p7+MYrcYpm0AFtKN2Ej2weXKMnd8mwrZMLC6k9IMFyQd9QJwvDIH5ogRjct43DZZiE5rSAxC+qnVViCnjcAtbdD+I3MgLgRZ3s3N2SLvjGXMkPmDr2NC9Ipb8ukOpXNByQ4ggfWrNZFAIVEg7KsSIN66h2TQhW0DHM7a9sFudLCmzWjam2jQshPFpSkuwSOpkUYjqNelxK6tE5Otf5CZGwJICbMHRN8HMEEgo=",
            "body": '{"Inactive": "N", "id": "CS00008707", "parentid": "CCSS008722", "name": "C&D CONSOLIDATED", "rootflag": "Y", "lvl": "5", "parentlvl": "4", "parentname": "CF R&D", "tdcindicator": "ABC", "companycode": "123"}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1594830519241",
                "SenderId": "AROA3OSU3UXXLZMDDJCX6:huang.sg.2@pg.com",
                "ApproximateFirstReceiveTimestamp": "1594831566760",
            },
            "messageAttributes": {},
            "md5OfBody": "fed5c04afd0d9720a70044e3ba9663c3",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:787230729710:workday-hierarchies-dev-costcenter-queue",
            "awsRegion": "us-east-1",
        },
        {
            "messageId": "0a5bfcd8-9e3f-435b-98ce-83608a2305be",
            "receiptHandle": "AQEBa0yAsIs0AYtmcmY113aLkpN8RrjCuAoE3pOBYn4bbj5YzSlYVbKDLHejkglqo9//mDooX7webIXqasSx9hf/Os5NKxRTcvQJpCPxwsfFclWYz1O5oggMraTegICE9zvW24i7kY2rV6iDOR/y5npyom3qaTzZaFqhPA8Kf398U6MjLdOBCYmsxa1dghbbsyefrUndvRO2/brJhy5ZC+J5ljLYRMh4Wh45Dy9jlB6HawD8t32+rLotuShjWUEDJv2xOGmtUfzOgT+/inleWqBzMw13nuHn+sdIzmrHDO9Rd/xRIBCyIc1cWVjeonqcv3qebSzc/9kBDxwCn1Jlay6t6m4z/MntawIOxjrortbUiRRM9dzWQGNMi17ubIVNDzUhoPdZ3kwGEit4qipqkRqSh0kQfrgieErxjwvSuFrGN9M=",
            "body": '{"Inactive": "N", "id": "PC787820", "parentid": "CP00000520", "name": "TR-TMS CUSTOMER- (B)", "rootflag": "Y", "lvl": "6", "parentlvl": "5", "parentname": "TDC LA MDO", "tdcindicator": "ABC", "companycode": "123"}',
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1594830519301",
                "SenderId": "AROA3OSU3UXXLZMDDJCX6:huang.sg.2@pg.com",
                "ApproximateFirstReceiveTimestamp": "1594831566760",
            },
            "messageAttributes": {},
            "md5OfBody": "c728b465fef512a4b0a258e58e40eb46",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:787230729710:workday-hierarchies-dev-costcenter-queue",
            "awsRegion": "us-east-1",
        },
    ]
}


def test_active_status(aws_credentials, secrets_manager, requests_mock):
    from process_deltas.send_soap_cc import get_active_status
    from process_deltas.helper_zeep import get_client

    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("get_cost_center_valid.xml"), status_code=200,
    )

    zeep, service, version = get_client("Testing", "Financial_Management")
    zeep.wsse = False

    assert get_active_status("1234", zeep, service, version)


def test_lambda_handler_failure_runtime(
    aws_credentials, sqs, requests_mock, secrets_manager, s3_error_bucket
):
    requests_mock.register_uri(
        "POST", TENANT_FM, text="error that can't be processed", status_code=500,
    )
    from process_deltas import send_soap_cc as send_soap
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap_cc import lambda_handler

    # mock the active status call since we can only mock one call
    def active_status(a, b, c, d):
        return "true"

    send_soap.get_active_status = active_status

    # mock the webservice call for Put_Cost_Center
    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing", "Financial_Management"
    )
    send_soap.zeep_client.wsse = False

    with pytest.raises(Exception) as exc_info:
        lambda_handler(event, context)

    assert (
        str(exc_info.value)
        == "Failing the Lambda to return any unprocessed items to SQS"
    )

    dt = date.today().strftime("%Y-%m-%d")
    client = boto3.client("s3")
    objects = client.list_objects(Bucket=os.environ.get("ERROR_BUCKET"))
    assert f"{dt}_costcenter_" in objects["Contents"][0]["Key"]


def test_lambda_handler_failure_handled(
    aws_credentials, sqs, requests_mock, secrets_manager, s3_error_bucket
):
    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("put_location_invalid.xml"), status_code=500,
    )
    from process_deltas import send_soap_cc as send_soap
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap_cc import lambda_handler

    # mock the active status call since we can only mock one call
    def active_status(a, b, c, d):
        return "true"

    send_soap.get_active_status = active_status

    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing", "Financial_Management"
    )
    send_soap.zeep_client.wsse = False

    lambda_handler(event, context)

    # check that all 3 handled failures are sent to s3
    client = boto3.client("s3")
    objects = client.list_objects(Bucket=os.environ.get("ERROR_BUCKET"))
    assert len(objects["Contents"]) == len(event["Records"])


def test_lambda_handler_success(
    aws_credentials, sqs, requests_mock, secrets_manager, s3_error_bucket
):
    requests_mock.register_uri(
        "POST", TENANT_FM, text=get_xml("put_cost_center_valid.xml"), status_code=200,
    )
    from process_deltas import send_soap_cc as send_soap
    from process_deltas.helper_zeep import get_client
    from process_deltas.send_soap_cc import lambda_handler

    # mock the active status call since we can only mock one call
    def active_status(a, b, c, d):
        return "true"

    send_soap.get_active_status = active_status

    send_soap.zeep_client, send_soap.zeep_service, send_soap.version = get_client(
        "Testing", "Financial_Management"
    )
    send_soap.zeep_client.wsse = False

    lambda_handler(event, context)
