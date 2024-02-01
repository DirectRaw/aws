import pytest
import json
import os
import requests
import moto
from mock import patch
import boto3

from get_data.tests.data import SAP_URL, Response

# doesn't test for paging results from SAP.

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
</SOAP:Envelope>""".strip()

one_field_response = """<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
    <SOAP:Header/>
    <SOAP:Body>
        <ns1:MT_TablesRead_Response xmlns:ns1='http://pg.com/xi/MDM/a2a/global/gedb'>
            <FieldDetails>
                <FieldName>GEBNR</FieldName>
                <FieldText>Building Number</FieldText>
            </FieldDetails>
            <Data>
                <Values>
                    <Item>000901</Item>
                    <Item>000902</Item>
                    <Item>000905</Item>
                </Values>
            </Data>
        </ns1:MT_TablesRead_Response>
    </SOAP:Body>
</SOAP:Envelope>""".strip()

field_mapping_adrc = {
    "STR_SUPPL3": "City_Subdivision_1",
    "CITY2": "Region_Subdivision_1",
    "ADDRNUMBER": "join_field",
}


def test_setup_sap_soap():
    """Formating of MT_TablesRead_Request SOAP envelop to SAP"""
    from get_data.helper_get import setup_sap_soap

    sap_target_system = "target_system"
    sap_table_name = "sap_table_name"
    sap_application_id = "sap_application_id"
    sap_select_fields = "sap_select_field"
    sap_where_field = "sap_where_field"
    row_skips = 0

    headers, payload = setup_sap_soap(
        sap_target_system,
        sap_table_name,
        sap_application_id,
        sap_select_fields,
        sap_where_field,
        row_skips,
    )

    assert (
        payload
        == f"""
        <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:gpdb=\"http://pg.com/xi/mdm/gpdb\">\r\n
            <soapenv:Header/>\r\n
            <soapenv:Body>\r\n
                <gpdb:MT_TablesRead_Request>\r\n
                    <ReadData>\r\n
                        <TargetSystem>{sap_target_system}</TargetSystem>\r\n
                        <TableName>{sap_table_name}</TableName>\r\n
                        <ApplicationID>{sap_application_id}</ApplicationID>\r\n
                        <SelectFields>{sap_select_fields.replace(" ", "")}</SelectFields>\r\n
                        <WhereField>{sap_where_field}</WhereField>\r\n
                        <RowCount>0</RowCount>\r\n
                        <RowSkips>{row_skips}</RowSkips>\r\n
                    </ReadData>\r\n
                </gpdb:MT_TablesRead_Request>\r\n
            </soapenv:Body>\r\n
        </soapenv:Envelope>
        """
    )

    assert headers == {
        "SOAPAction": '"http://sap.com/xi/WebService/soap1.1"',
        "Content-Type": "application/xml",
    }


def test_process_response_multiplefields_mappings():
    """Format and change column names of MT_TablesRead_Response for multiple fields"""
    from get_data.helper_get import process_response

    results = []

    num_responses = process_response(Response(adrc_response, 200), results, field_mapping_adrc)

    assert num_responses == 3
    assert results == [
        {
            "join_field": "0000000001",
            "City_Subdivision_1": "CITY_SUBDIVISION_1",
            "Region_Subdivision_1": "REGION_SUBDIVISION_1",
        },
        {
            "join_field": "0000000002",
            "City_Subdivision_1": "CITY_SUBDIVISION_2",
            "Region_Subdivision_1": "REGION_SUBDIVISION_2",
        },
        {
            "join_field": "0000000004",
            "City_Subdivision_1": "CITY_SUBDIVISION_3",
            "Region_Subdivision_1": "REGION_SUBDIVISION_3",
        },
    ]


def test_process_response_multiplefields_nomappings():
    """Format without changing column names of MT_TablesRead_Response for multiple fields"""
    from get_data.helper_get import process_response

    results = []

    num_responses = process_response(Response(adrc_response, 200), results, None)

    assert num_responses == 3
    assert results == [
        {
            "ADDRNUMBER": "0000000001",
            "STR_SUPPL3": "CITY_SUBDIVISION_1",
            "CITY2": "REGION_SUBDIVISION_1",
        },
        {
            "ADDRNUMBER": "0000000002",
            "STR_SUPPL3": "CITY_SUBDIVISION_2",
            "CITY2": "REGION_SUBDIVISION_2",
        },
        {
            "ADDRNUMBER": "0000000004",
            "STR_SUPPL3": "CITY_SUBDIVISION_3",
            "CITY2": "REGION_SUBDIVISION_3",
        },
    ]


def test_process_response_singlefield_mappings():
    """Format by changing column names of MT_TablesRead_Response for single field"""
    from get_data.helper_get import process_response

    results = []

    num_responses = process_response(
        Response(one_field_response, 200), results, {"GEBNR": "COLUMN"}
    )

    assert num_responses == 3
    assert results == [{"COLUMN": "000901"}, {"COLUMN": "000902"}, {"COLUMN": "000905"}]


def test_process_response_singlefield_nomappings():
    """Format without changing column names of MT_TablesRead_Response for single field"""
    from get_data.helper_get import process_response

    results = []

    num_responses = process_response(Response(one_field_response, 200), results, None)

    assert num_responses == 3
    assert results == [{"GEBNR": "000901"}, {"GEBNR": "000902"}, {"GEBNR": "000905"}]


def test_process_response_bad_mapping():
    """Format column names of MT_TablesRead_Response for single field"""
    from get_data.helper_get import process_response

    results = []

    with pytest.raises(Exception) as exc_info:
        process_response(Response(one_field_response, 200), results, {"Unknown Field": "Mapping"})

    assert str(exc_info.value) == "unknown field header mapping, please update"


@pytest.mark.dependency()
def test_write_sap_json():
    """Format the response from SAP and write in Athena readable format into local temp file"""
    from get_data.helper_get import write_sap_json

    output_path = "stack/get_data/tests/data/test.json"
    results = [
        {
            "join_field": "0000000001",
            "City_Subdivision_1": "CITY_SUBDIVISION_1",
            "Region_Subdivision_1": "REGION_SUBDIVISION_1",
        },
        {
            "join_field": "0000000002",
            "City_Subdivision_1": "CITY_SUBDIVISION_2",
            "Region_Subdivision_1": "REGION_SUBDIVISION_2",
        },
        {
            "join_field": "0000000004",
            "City_Subdivision_1": "CITY_SUBDIVISION_3",
            "Region_Subdivision_1": "REGION_SUBDIVISION_3",
        },
    ]

    write_sap_json(results, output_path)


@pytest.mark.dependency(depends=["test_write_sap_json"])
def test_upload_file(aws_credentials, s3_data_bucket):
    """Upload the file to S3"""
    from get_data.helper_get import upload_file

    input_file = "stack/get_data/tests/data/test.json"
    bucket = os.environ.get("BUCKET")
    prefix = os.environ.get("SITE_NEW")
    filename = "test.json"

    upload_file(input_file, bucket, prefix, filename)

    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=f"{prefix}{filename}")
    with open(input_file, "r") as f:
        assert f.read() == response["Body"].read().decode("utf-8")


@pytest.mark.dependency(depends=["test_write_sap_json"])
def test_upload_file_error(aws_credentials, s3_data_bucket):
    """Error uploading the file to S3"""
    from get_data.helper_get import upload_file

    input_file = "stack/get_data/tests/data/test.json"
    bucket = "UNKNOWN BUCKET"
    prefix = os.environ.get("SITE_NEW")
    filename = "test.json"

    with pytest.raises(Exception) as exc_info:
        upload_file(input_file, bucket, prefix, filename)

    assert "Parameter validation failed:" in str(exc_info.value)


def test_sap_post_req_success(aws_credentials, secrets_manager, requests_mock):
    """Send a SOAP envelope to SAP and return processed results. Creds stored in Secretsmanager"""
    requests_mock.register_uri("POST", SAP_URL, text=adrc_response, status_code=200)

    from get_data.helper_get import sap_post_req

    sap_table_name = "sap_table_name"
    sap_select_fields = "sap_select_field"
    sap_where_field = "sap_where_field"

    results = sap_post_req(sap_table_name, sap_select_fields, sap_where_field, field_mapping_adrc)

    assert results == [
        {
            "join_field": "0000000001",
            "City_Subdivision_1": "CITY_SUBDIVISION_1",
            "Region_Subdivision_1": "REGION_SUBDIVISION_1",
        },
        {
            "join_field": "0000000002",
            "City_Subdivision_1": "CITY_SUBDIVISION_2",
            "Region_Subdivision_1": "REGION_SUBDIVISION_2",
        },
        {
            "join_field": "0000000004",
            "City_Subdivision_1": "CITY_SUBDIVISION_3",
            "Region_Subdivision_1": "REGION_SUBDIVISION_3",
        },
    ]


def test_sap_post_req_failure(aws_credentials, secrets_manager, requests_mock):
    """Unauthorized Error in SAP"""
    requests_mock.register_uri(
        "POST", SAP_URL, text="here is my error", status_code=500,
    )

    from get_data.helper_get import sap_post_req

    sap_table_name = "sap_table_name"
    sap_select_fields = "sap_select_field"
    sap_where_field = "sap_where_field"

    with pytest.raises(Exception) as exc_info:
        sap_post_req(sap_table_name, sap_select_fields, sap_where_field, field_mapping_adrc)

    assert str(exc_info.value) == "b'here is my error'"
