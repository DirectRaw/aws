try:
    import unzip_requirements
except ImportError:
    pass

import json
import os
from datetime import date

import boto3

from process_deltas.helper_send import send_web_service, write_error_s3
from process_deltas.helper_soap import add_update_org
from process_deltas.helper_zeep import get_client, send_request
from shared.helper import logger


# these should go to event context
zeep_client = None
zeep_service = None
version = None


def get_active_status(costcenter_id, zeep_client, zeep_service, version):
    """Gets the Active_Organization status of a Cost Center in Workday. Returns 'true' or 'false'"""
    return send_web_service(
        "GET_ACTIVE_COSTCENTERS", costcenter_id, zeep_client, zeep_service, version
    )


def lambda_handler(event, context):
    """This is special case due to the volume of cost centers we have: SQS triggers Lambda
        https://www.serverless.com/blog/aws-lambda-sqs-serverless-integration
        
        Cost centers also uses WD Financial Management Service
    """

    hierarchy = event["hierarchy"] = "costcenter"
    error_bucket = os.environ.get("ERROR_BUCKET")

    # get the date for error logs
    dt = date.today().strftime("%Y-%m-%d")
    event["date"] = dt
    msg = None

    # set up Zeep if not defined in event context
    global zeep_client
    global zeep_service
    global version

    try:
        if zeep_client is None or zeep_service is None or version is None:
            zeep_client, zeep_service, version = get_client(False, "Financial_Management")

        # get step function variables
        for msg in event["Records"]:
            msg_id = msg["messageId"]
            msg_receipt = msg["receiptHandle"]
            body = json.loads(msg["body"])

            # get the active status first because Put_Cost_Centers will default to Inactive and set in data
            logger.info(f'Getting Organization_Active Status for {body["id"]}')
            body["active_in_wd"] = get_active_status(
                body["id"], zeep_client, zeep_service, version
            )

            # post web service to Workday
            logger.info(f'Sending {hierarchy}: {body["id"]} to WD')
            response = send_web_service(hierarchy, body, zeep_client, zeep_service, version)

            # if successfully sent then pass. SQS + Lambda trigger will mark entire batch processed if Lambda succeeds
            if response is True:
                logger.info(f'Successfully sent {body["id"]}')
            else:
                # otherwise we will handle the error.
                logger.error(response)
                logger.error(
                    f'Failed to send {hierarchy}: {body["id"]} to WD, uploading error to S3'
                )
                # log the error into s3, continue to next item because if web service failed, retrying won't help
                error_body = {"body": {"id": body["id"]}, "SQS": body}
                write_error_s3(error_bucket, event, context, error_body, response)

    except Exception as e:
        # likely if we hit an exception while running this, none of them will succeed
        logger.error(f"Runtime Error ({e}), uploading error to S3")

        key = (
            f'{event["date"]}/{hierarchy}/{event["date"]}_{hierarchy}_{context.aws_request_id}.txt'
        )

        # add the data that we received from SQS
        error_body = f"Error:\n{str(e)}\n\n"
        if msg:
            error_body += f"Data from SQS:\n{json.dumps(msg, indent=4)}\n\n"

        # add to S3 into the error logging bucket
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=error_bucket,
            Key=key,
            Body=error_body,
            ACL="bucket-owner-full-control",
            ServerSideEncryption="AES256",
        )

        raise Exception("Failing the Lambda to return any unprocessed items to SQS")

