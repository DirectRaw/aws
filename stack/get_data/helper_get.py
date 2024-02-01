import os
import json
import requests
import xmltodict

# import cx_Oracle
import boto3
from botocore.exceptions import ClientError, ParamValidationError
from tempfile import NamedTemporaryFile as NamedTempFile
from requests.auth import HTTPBasicAuth

try:
    from shared.helper import get_secrets, logger
except ModuleNotFoundError:
    import sys

    sys.path.append("../")
    from shared.helper import get_secrets, logger

SAP_MAX_RESULTS = 1000

# export LD_LIBRARY_PATH=~/Projects/workday-hierarchies/stack/layers/oracle_instant_client/lib


# def get_rds(query_path, output_path):
#     """connect to RDS using Oracle and executes a query"""

#     # get secrets
#     secrets = get_secrets(False)
#     rds_user = secrets["rds_username"]
#     rds_pwd = secrets["rds_password"]
#     rds_db_name = secrets["rds_db_name"]
#     rds_host = secrets["rds_host"]
#     rds_port = secrets["rds_port"]
#     rds_query = open(query_path).read()

#     try:
#         dns_str = cx_Oracle.makedsn(rds_host, rds_port, rds_db_name)
#         with cx_Oracle.connect(user=rds_user, password=rds_pwd, dsn=dns_str) as conn:
#             cursor = conn.cursor()
#             cursor.execute(rds_query)
#             with open(output_path, "w") as f:
#                 for result in cursor:
#                     f.write(result[0].replace("null", '""'))
#                     f.write("\n")
#     except cx_Oracle.DatabaseError as e:
#         raise e


def sap_post_req(
    sap_table_name, sap_select_fields, sap_where_field, field_mapping, local=False
):
    """Sends a MT_TablesRead_Request query to SAP PI"""

    # get secrets
    secrets = get_secrets(False)
    sap_username = secrets["sap_username"]
    sap_password = secrets["sap_password"]
    sap_host = secrets["sap_host"]
    sap_url = secrets["sap_url"]
    sap_pipo_url = f"https://{sap_host}{sap_url}"
    sap_target_system = secrets["sap_target_system"]
    sap_application_id = secrets["sap_application_id"]
    sap_client_cert = secrets["sap_client_cert"]
    sap_client_key = secrets["sap_client_private_key"]

    # path is different when deployed vs locally
    path = "certificates/" if local else "get_data/certificates/"
    sap_ssl_cert = path + secrets["sap_ssl_cert"]

    # variables for paging results
    count = SAP_MAX_RESULTS
    row_skips = 0
    results = []

    # loop through until we get less than 1000 records, then it's the last page
    while not count < SAP_MAX_RESULTS:

        # get the format of the SOAP call
        headers, payload = setup_sap_soap(
            sap_target_system,
            sap_table_name,
            sap_application_id,
            sap_select_fields,
            sap_where_field,
            row_skips,
        )

        # set up temporary file paths for requests to read in certificates
        client_cert = NamedTempFile(delete=False, prefix="certificates", suffix=".pem")
        client_cert.write(sap_client_cert.encode())
        client_cert.close()
        private_key = NamedTempFile(delete=False, prefix="certificates", suffix=".pem")
        private_key.write(sap_client_key.encode())
        private_key.close()

        # send the request to SAP PI
        try:
            response = requests.request(
                method="POST",
                url=sap_pipo_url,
                headers=headers,
                data=payload,
                auth=HTTPBasicAuth(sap_username, sap_password),
                verify=sap_ssl_cert,
                cert=(client_cert.name, private_key.name),
            )
        except requests.exceptions.ConnectionError as e:
            raise Exception("failed to connect")
        except OSError as e:
            raise Exception("cert error")
        finally:
            # the temp files need to be explicitly deleted, this is security flaw
            os.unlink(client_cert.name)
            os.unlink(private_key.name)

        if response.status_code == 200:
            # add this page to results while formatting, get count of most recent results
            count = process_response(response, results, field_mapping)
            row_skips += count
        else:
            # SAP will return an HTML on auth error but SOAP response on server error
            raise Exception(response.content)

    return results


def setup_sap_soap(
    sap_target_system,
    sap_table_name,
    sap_application_id,
    sap_select_fields,
    sap_where_field,
    row_skips,
):
    """populates the SOAP xml with input parameters to form request to SAP"""
    headers = {
        "SOAPAction": '"http://sap.com/xi/WebService/soap1.1"',
        "Content-Type": "application/xml",
    }

    payload = f"""
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

    return headers, payload


def process_response(response, results, field_mapping):
    """processes a result of an SAP SOAP call. returns the total count"""

    # process the SOAP response as a dict
    resp_dict = xmltodict.parse(response.content)
    body = resp_dict["SOAP:Envelope"]["SOAP:Body"]["ns1:MT_TablesRead_Response"]
    fields = body["FieldDetails"]
    rows = body["Data"]["Values"]["Item"]

    # update the SAP header fields to match Workday formatting downstream
    column_names = map_columns(fields, field_mapping)

    # write out the rows as dictionary with field names
    for row in rows:
        column_values = [value.strip() for value in row.split("|")]
        results.append(dict(zip(column_names, column_values)))

    # return the number of results in this page, anything under 1000 means it's last page
    return len(rows)


def map_columns(fields, field_mapping):
    """"maps the SAP column names to whatever the WD field mapping is - doesn't work on single field single result"""
    try:
        if len(fields) > 2:
            if field_mapping:
                return [field_mapping[field["FieldName"]] for field in fields]
            else:
                return [field["FieldName"] for field in fields]
        else:
            if field_mapping:
                return [field_mapping[fields["FieldName"]]]
            else:
                return [fields["FieldName"]]

    except KeyError:
        raise Exception("unknown field header mapping, please update")
    except TypeError:
        raise Exception("no field headers?")


def write_sap_json(results, output_path):
    """write locally the SAP output into a JSON format that Athena can read"""
    with open(output_path, "w") as f:
        for row in results:
            f.write(json.dumps(row, separators=(",", ":")))
            f.write("\n")


def upload_file(input, bucket, prefix, filename):
    """upload a local file to  specified s3 prefix"""
    logger.info(f"Uploading {filename} to {bucket}/{prefix}")
    client = boto3.client("s3")
    try:
        client.upload_file(
            Filename=input,
            Bucket=bucket,
            Key=f"{prefix}{filename}",
            ExtraArgs={"ServerSideEncryption": "AES256", "ACL": "private"},
        )
    except (ClientError, ParamValidationError) as e:
        raise e
