try:
    import unzip_requirements
except ImportError:
    pass

import json
import os

import boto3

try:
    from process_deltas.helper_zeep import send_request
    from process_deltas.helper_soap import (
        add_update_org,
        put_location,
        get_locations,
        put_cost_center,
        get_cost_center,
    )
    from shared.helper import logger

except ModuleNotFoundError:
    import sys
    from helper_zeep import send_request
    from helper_soap import (
        add_update_org,
        put_location,
        get_locations,
        put_cost_center,
        get_cost_center,
    )

    sys.path.append("../")
    from shared.helper import logger


def send_web_service(hierarchy, fields, zeep_client, zeep_service, version):
    """
    Send the row of data from SQS to Workday using the respetive SOAP call
        1. Gets the dict that represents the SOAP call populated with the row data
        2. Sends the SOAP envelope with Zeep
        
    Returns True if successful, otherwise returns the error message
    """

    if hierarchy == "ninetwosix":
        payload, operation = add_update_org("Cost Center Hierarchy", fields)
    elif hierarchy == "ninetwoone":
        payload, operation = add_update_org("Location Hierarchy", fields)
    elif hierarchy == "onezeroone":
        payload, operation = add_update_org("Company Hierarchy", fields)
    elif hierarchy == "costcenter":
        payload, operation = put_cost_center(fields)
    elif hierarchy == "companycode":
        payload, operation = add_update_org("Company", fields)
    elif hierarchy == "site":
        payload, operation = put_location("Site", fields)
    elif hierarchy == "building":
        payload, operation = put_location("Building", fields)
    elif hierarchy == "GET_INACTIVE_SITE":
        payload, operation = get_locations("Site", fields)
    elif hierarchy == "GET_INACTIVE_BUILDING":
        payload, operation = get_locations("Building", fields)
    elif hierarchy == "GET_ACTIVE_COSTCENTERS":
        payload, operation = get_cost_center(fields)
    else:
        raise Exception("Unknown web service")

    return send_request(operation, zeep_client, zeep_service, version, payload)


def write_error_s3(error_bucket, event, context, msg, response):
    """Build error file and write to S3 - this will only be this detailed if the error is caused by the WD Call"""

    # get variables from event
    date = event["date"]
    hierarchy = event["hierarchy"]

    # organizing prefixes by date -> hierarchy
    # then filename will be YYYY-DD-MM_HIERARCHY_ID.txt
    key = f'{date}/{hierarchy}/{date}_{hierarchy}_{msg["body"]["id"]}.txt'

    # get the reference id
    body = "ID:\n"
    body += msg["body"]["id"]
    body += "\n\n"

    # get the cloudwatch logging data from context
    body += "Logs:\n"
    body += f"{context.log_group_name}\n"
    body += f"{context.log_stream_name}\n\n"

    # add the error response from WD, can also append the SOAP envelope that we sent to WD
    body += f"Response:\n"
    for line in response.split("\n"):
        body += f"{line}\n"
    body += "\n"

    # add the data that we received from SQS
    body += f"Data from SQS:\n{json.dumps(msg, indent=4)}\n\n"

    # add to S3 into the error logging bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=error_bucket,
        Key=key,
        Body=body,
        ACL="bucket-owner-full-control",
        ServerSideEncryption="AES256",
    )
