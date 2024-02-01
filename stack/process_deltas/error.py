try:
    import unzip_requirements
except ImportError:
    pass

import json
import os

import boto3

from shared.helper import get_secrets, logger


def lambda_handler(event, context):
    """Error handling state"""

    # get event and environ variables
    hierarchy = event["hierarchy"]
    date = event["date"]
    error_bucket = os.environ.get("ERROR_BUCKET")

    # get the error messaged raised by Exception
    error = json.loads(event["error"]["Cause"])

    if "logged in s3" in error["errorMessage"]:
        logger.warning("Caught error, already logged in S3, ending because no_fail=False")
    else:
        # this is runtime error from step function and will stop the step function, log to s3
        key = f"{date}/{hierarchy}/{date}_{hierarchy}_error_{context.aws_request_id}.txt"

        # build report
        body = f"Hierarchy: {hierarchy}\n"
        try:
            body += f'Total Deltas: {event["deltas_length"]}\n'
        except KeyError:
            body += f"Total Deltas: none\n"
        try:
            body += f'Queue Length: {event["queue_length"]}\n'
        except KeyError:
            body += f"Queue Length: none\n"
        body += f"Error: {json.dumps(error, indent=4)}\n"

        # send to s3
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=error_bucket,
            Key=key,
            Body=body,
            ACL="bucket-owner-full-control",
            ServerSideEncryption="AES256",
        )
