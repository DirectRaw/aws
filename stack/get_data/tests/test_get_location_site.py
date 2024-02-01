import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3
from get_data.tests.data import SAP_URL, get_xml

t001w_response = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>WERKS</FieldName>
                <FieldText>Plant</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>NAME1</FieldName>
                <FieldText>Name</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>ZZSITETYPC</FieldName>
                <FieldText>Global Site Type Code</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>LAND1</FieldName>
                <FieldText>Country Key</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>STRAS</FieldName>
                <FieldText>House number and street</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>ORT01</FieldName>
                <FieldText>City</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>REGIO</FieldName>
                <FieldText>Region (State, Province, County)</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>PSTLZ</FieldName>
                <FieldText>Postal Code</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>SPRAS</FieldName>
                <FieldText>Language Key</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>ADRNR</FieldName>
                <FieldText>Address</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>ZZSITESTCD</FieldName>
                <FieldText>Global Site Status Code</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>0009|6TH OF OCTOBER CITY PLANT     |P   |EG |INDUSTRIAL ZONE NUMBER 1      |6TH OCTOBER CITY         |SU |001101    |E|0000000001|A</Item>
                    <Item>0014|PROSPECTIVE SITES-CINCI-AGILE |P   |US |2 PROCTER &#38; GAMBLE PLAZA  |CINCINNATI               |OH |45202     |E|0000000002|A</Item>
                    <Item>0023|ST PETERSBURG TC-LONDA        |OFF |RU |BASKOV PEREYLOK 26A           |ST PETERSBURG            |   |          |E|0000000003|C</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>""".strip()

adrc_response = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>ADDRNUMBER</FieldName>
                <FieldText>Address number</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>STR_SUPPL3</FieldName>
                <FieldText>Street 4</FieldText>
            </FieldDetails>
            <FieldDetails>
                <FieldName>CITY2</FieldName>
                <FieldText>District</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>0000000001|               CITY_SUBDIVISION_1|         REGION_SUBDIVISION_1</Item>
                    <Item>0000000002|               CITY_SUBDIVISION_2|         REGION_SUBDIVISION_2</Item>
                    <Item>0000000004|               CITY_SUBDIVISION_3|         REGION_SUBDIVISION_3</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>
""".strip()


def test_get_adrc(aws_credentials, s3_data_bucket, secrets_manager, requests_mock):
    """Get city_subdivision_1 and region_subdivision_1 for sites"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=adrc_response, status_code=200,
    )
    from get_data.get_location_site import get_adrc

    assert get_adrc() == {
        "0000000001": {
            "City_Subdivision_1": "CITY_SUBDIVISION_1",
            "Region_Subdivision_1": "REGION_SUBDIVISION_1",
        },
        "0000000002": {
            "City_Subdivision_1": "CITY_SUBDIVISION_2",
            "Region_Subdivision_1": "REGION_SUBDIVISION_2",
        },
        "0000000004": {
            "City_Subdivision_1": "CITY_SUBDIVISION_3",
            "Region_Subdivision_1": "REGION_SUBDIVISION_3",
        },
    }


def test_get_companies_to_keep(
    aws_credentials, s3_data_bucket, secrets_manager, requests_mock
):
    """Get all sites"""
    requests_mock.register_uri(
        "POST", SAP_URL, text=t001w_response, status_code=200,
    )

    from get_data.get_location_site import get_t001w

    assert get_t001w() == [
        {
            "ID": "0009",
            "Name": "6TH OF OCTOBER CITY PLANT",
            "Location_Type_ID": "P",
            "Country": "EG",
            "Address_Line_1": "INDUSTRIAL ZONE NUMBER 1",
            "Municipality": "6TH OCTOBER CITY",
            "Region": "SU",
            "Postal_Code": "001101",
            "User_Language_ID": "E",
            "join_field": "0000000001",
            "status": "A",
        },
        {
            "ID": "0014",
            "Name": "PROSPECTIVE SITES-CINCI-AGILE",
            "Location_Type_ID": "P",
            "Country": "US",
            "Address_Line_1": "2 PROCTER & GAMBLE PLAZA",
            "Municipality": "CINCINNATI",
            "Region": "OH",
            "Postal_Code": "45202",
            "User_Language_ID": "E",
            "join_field": "0000000002",
            "status": "A",
        },
        {
            "ID": "0023",
            "Name": "ST PETERSBURG TC-LONDA",
            "Location_Type_ID": "OFF",
            "Country": "RU",
            "Address_Line_1": "BASKOV PEREYLOK 26A",
            "Municipality": "ST PETERSBURG",
            "Region": "",
            "Postal_Code": "",
            "User_Language_ID": "E",
            "join_field": "0000000003",
            "status": "C",
        },
    ]


def test_lambda_handler(
    aws_credentials, s3_data_bucket, secrets_manager, requests_mock
):
    """
    Site 0023 does not have matching join_field in ADRC, 
        so City_Subdivision_1 and Region_Subdivison_1 are empty
    
    Site 0009 and 0014 should have corresponding fields from ADRC
    """

    # mock t001w response
    requests_mock.register_uri(
        "POST", SAP_URL, text=t001w_response, status_code=200,
    )
    from get_data import get_location_site
    from get_data.get_location_site import lambda_handler

    # can only mock one request to endpoint so patching post request to ADRC
    def get_adrc_modified(local):
        return {
            "0000000001": {
                "City_Subdivision_1": "CITY_SUBDIVISION_1",
                "Region_Subdivision_1": "REGION_SUBDIVISION_1",
            },
            "0000000002": {
                "City_Subdivision_1": "CITY_SUBDIVISION_2",
                "Region_Subdivision_1": "REGION_SUBDIVISION_2",
            },
            "0000000004": {
                "City_Subdivision_1": "CITY_SUBDIVISION_3",
                "Region_Subdivision_1": "REGION_SUBDIVISION_3",
            },
        }

    get_location_site.get_adrc = get_adrc_modified

    # tmp directory not located at highest level where tests are being run, so create it
    try:
        os.mkdir("tmp/")
    except FileExistsError:
        pass
    finally:
        lambda_handler({}, "")

    # check that the file stored into s3 matches the formatting we want for athena
    client = boto3.client("s3")
    response = client.get_object(
        Bucket=os.environ.get("BUCKET"), Key=f'{os.environ.get("SITE_NEW")}site.json',
    )
    assert (
        response["Body"].read().decode("utf-8")
        == """{"ID":"0009","Name":"6TH OF OCTOBER CITY PLANT","Location_Type_ID":"P","Country":"EG","Address_Line_1":"INDUSTRIAL ZONE NUMBER 1","Municipality":"6TH OCTOBER CITY","Region":"SU","Postal_Code":"001101","User_Language_ID":"en_US","status":"A","Location_Usage_Type":"BUSINESS SITE","ParentId":"0009","Default_Currency_ID":"","Time_Profile_ID":"","Address_Line_2":"","Region_Subdivision_1":"REGION_SUBDIVISION_1","City_Subdivision_1":"CITY_SUBDIVISION_1"}
{"ID":"0014","Name":"PROSPECTIVE SITES-CINCI-AGILE","Location_Type_ID":"P","Country":"US","Address_Line_1":"2 PROCTER & GAMBLE PLAZA","Municipality":"CINCINNATI","Region":"OH","Postal_Code":"45202","User_Language_ID":"en_US","status":"A","Location_Usage_Type":"BUSINESS SITE","ParentId":"0014","Default_Currency_ID":"","Time_Profile_ID":"","Address_Line_2":"","Region_Subdivision_1":"REGION_SUBDIVISION_2","City_Subdivision_1":"CITY_SUBDIVISION_2"}
{"ID":"0023","Name":"ST PETERSBURG TC-LONDA","Location_Type_ID":"OFF","Country":"RU","Address_Line_1":"BASKOV PEREYLOK 26A","Municipality":"ST PETERSBURG","Region":"","Postal_Code":"","User_Language_ID":"en_US","status":"C","Location_Usage_Type":"BUSINESS SITE","ParentId":"0023","Default_Currency_ID":"","Time_Profile_ID":"","Address_Line_2":"","Region_Subdivision_1":"","City_Subdivision_1":""}
"""
    )
