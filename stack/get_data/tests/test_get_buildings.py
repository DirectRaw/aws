import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3
from get_data.tests.data import SAP_URL

building_response = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>GEBNR</FieldName>
                <FieldText>Building Number</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>ZBLDGNAME</FieldName>
                <FieldText>Building Name</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>LAND1</FieldName>
                <FieldText>Country Key</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>000901|October 6 Plant               |EG</Item>
                    <Item>001400|Rio de Janeiro Plant          |BR</Item>
                    <Item>000000|Beijing Innovation Ctr-Tianzhu|CN</Item>                    <!-- This one won't have a site match-->
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>""".strip()

t001w_response_werks = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>WERKS</FieldName>
                <FieldText>Plant</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>0009</Item>
                    <Item>0014</Item>
                    <Item>0023</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>""".strip()


def test_get_buildings(aws_credentials, s3_data_bucket, secrets_manager, requests_mock):
    """Get all the buildings"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=building_response, status_code=200,
    )

    from get_data.get_buildings import get_buildings

    assert get_buildings() == [
        {"id": "000901", "name": "October 6 Plant", "country": "EG"},
        {"id": "001400", "name": "Rio de Janeiro Plant", "country": "BR"},
        {"id": "000000", "name": "Beijing Innovation Ctr-Tianzhu", "country": "CN"},
    ]


def test_get_site(aws_credentials, s3_data_bucket, secrets_manager, requests_mock):
    """Get active sites for filtering out buildings"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=t001w_response_werks, status_code=200,
    )

    from get_data.get_buildings import get_sites

    assert get_sites() == ["0009", "0014", "0023"]


def test_lambda_handler(
    aws_credentials, s3_data_bucket, secrets_manager, requests_mock
):
    """
    Lambda handler which pulls buildings from G11
    
    In this test scenario there is one building which does not have an active site, and we are only getting 
    buildings associated with sites so there should only be 2 results uploaded to s3.
    
    e.g. in ztxxetbldg_response.xml, id=000000 does not have a corresponding site in t001_response_werks.xml
    so it will not appera in the final output.
    """

    requests_mock.register_uri(
        "POST", SAP_URL, text=building_response, status_code=200,
    )

    from get_data import get_buildings
    from get_data.get_buildings import lambda_handler

    # can only mock one request to endpoint so patching one of the post requests
    def get_sites_modified(local):
        return ["0009", "0014", "0023"]

    get_buildings.get_sites = get_sites_modified

    # tmp directory not located at highest level where tests are being run, so create it
    try:
        os.mkdir("tmp/")
    except FileExistsError:
        pass
    finally:
        lambda_handler({}, "")

    client = boto3.client("s3")
    response = client.get_object(
        Bucket=os.environ.get("BUCKET"),
        Key=f'{os.environ.get("BUILDING_NEW")}building.json',
    )
    assert (
        response["Body"].read().decode("utf-8")
        == """{"id":"000901","name":"October 6 Plant","country":"EG","Location_Usage_Type":"WORK SPACE","Location_Type_ID":"B","parentid":"0009","Time_Profile_ID":"","Default_Currency_ID":""}
{"id":"001400","name":"Rio de Janeiro Plant","country":"BR","Location_Usage_Type":"WORK SPACE","Location_Type_ID":"B","parentid":"0014","Time_Profile_ID":"","Default_Currency_ID":""}
"""
    )
