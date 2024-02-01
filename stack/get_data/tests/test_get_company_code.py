import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3
from get_data.tests.data import SAP_URL

t001_response = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>BUKRS</FieldName>
                <FieldText>Company Code</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>BUTXT</FieldName>
                <FieldText>Name of Company Code or Company</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>WAERS</FieldName>
                <FieldText>Currency Key</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>001 |The P&#38;G Company          |USD</Item>
                    <Item>002 |P&#38;G Manufacturing Co.    |USD</Item>
                    <Item>003 |The P&#38;G Distributing LLC |USD</Item>
                    <Item>004 |P&#38;G Productions Inc.     |USD</Item>
                    <Item>542 |P&#38;G (EGYPT) IND &#38; COMM   |EGP</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>""".strip()

t001z_response = """
<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>BUKRS</FieldName>
                <FieldText>Company Code</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>001</Item>
                    <Item>002</Item>
                    <Item>003</Item>
                    <Item>004</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>
"""


def test_get_companies(aws_credentials, s3_data_bucket, secrets_manager, requests_mock):
    """Get all of the companies"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=t001_response, status_code=200,
    )

    from get_data.get_company_code import get_companies

    assert get_companies() == [
        {"id": "001", "name": "The P&G Company", "currencycode": "USD"},
        {"id": "002", "name": "P&G Manufacturing Co.", "currencycode": "USD"},
        {"id": "003", "name": "The P&G Distributing LLC", "currencycode": "USD"},
        {"id": "004", "name": "P&G Productions Inc.", "currencycode": "USD"},
        {"id": "542", "name": "P&G (EGYPT) IND & COMM", "currencycode": "EGP"},
    ]


def test_get_companies_to_keep(
    aws_credentials, s3_data_bucket, secrets_manager, requests_mock
):
    """Get the list of company codes that we want to filter on"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=t001z_response, status_code=200,
    )

    from get_data.get_company_code import get_companies_to_keep

    assert get_companies_to_keep() == ["001", "002", "003", "004"]


def test_lambda_handler(
    aws_credentials, s3_data_bucket, secrets_manager, requests_mock
):
    """companycode_id=542 from T001 should not be in final output because it's not in T001Z"""

    requests_mock.register_uri(
        "POST", SAP_URL, text=t001_response, status_code=200,
    )
    from get_data import get_company_code
    from get_data.get_company_code import lambda_handler

    # can only mock one request to endpoint so patching one of the post requests
    def get_companies_to_keep_modified(local):
        return ["001", "002", "003", "004"]

    get_company_code.get_companies_to_keep = get_companies_to_keep_modified

    # tmp directory not located at highest level where tests are being run, so create it
    try:
        os.mkdir("tmp/")
    except FileExistsError:
        pass
    finally:
        lambda_handler({}, "")

    # check that the data uploaded to s3 matches
    client = boto3.client("s3")
    response = client.get_object(
        Bucket=os.environ.get("BUCKET"),
        Key=f'{os.environ.get("COMPANYCODE_NEW")}companycode.json',
    )

    assert (
        response["Body"].read().decode("utf-8")
        == """{"id":"001","name":"The P&G Company"}
{"id":"002","name":"P&G Manufacturing Co."}
{"id":"003","name":"The P&G Distributing LLC"}
{"id":"004","name":"P&G Productions Inc."}
"""
    )
