try:
    import unzip_requirements
except ImportError:
    pass

import json
import os

import boto3

from process_deltas.helper_boto3 import (
    sqs_delete_message_batch,
    sqs_get_queue_length,
    sqs_receive_message_batch,
)
from process_deltas.helper_send import send_web_service, write_error_s3
from process_deltas.helper_validation import validate_site
from process_deltas.helper_zeep import get_client
from shared.helper import logger

# these should go to event context
zeep_client = None
zeep_service = None
version = None


def lambda_handler(event, context):

    # get variables from event and environment
    hierarchy = event["hierarchy"]
    date = event["date"]
    sqs_url = os.environ.get(f"{hierarchy.upper()}_SQS_URL")
    no_fails = os.environ.get("NO_FAILS")
    error_bucket = os.environ.get("ERROR_BUCKET")
    stop_when_fail = ["ninetwosix", "onezeroone", "ninetwoone", "companycode", "site"]

    # get message from queue
    sqs_client = boto3.client("sqs")
    messages = sqs_receive_message_batch(sqs_client, sqs_url)
    logger.info(f"Retrieved {len(messages)} from SQS")

    # set up Zeep if not defined in event context
    global zeep_client
    global zeep_service
    global version
    if zeep_client is None or zeep_service is None or version is None:
        zeep_client, zeep_service, version = get_client(context == "")

    # process all of the messages from s!S
    for msg in messages:
        msg_id = msg["body"]["id"]
        msg_receipt = msg["delete"]["ReceiptHandle"]

        if hierarchy == "site":
            # special case for sites because we need to get time profile first
            processed = process_site(
                event, context, msg, zeep_client, zeep_service, version, error_bucket, no_fails,
            )
            if not processed:
                # if time_profile_reference == False, then the site is inactive and we skip it
                sqs_client.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg_receipt)
                continue

        # we also need to check for buildings whether they are already active in wd
        elif hierarchy == "building":
            inactive = send_web_service(
                "GET_INACTIVE_BUILDING", msg_id, zeep_client, zeep_service, version,
            )
            msg["body"]["inactive_in_wd"] = "true" if inactive else "false"

        # post web service to Workday
        logger.info(f"Sending {hierarchy}: {msg_id} to WD")
        response = send_web_service(hierarchy, msg["body"], zeep_client, zeep_service, version)

        # if successfully sent, mark processed to delete from queue
        if response is True:
            logger.info(f"Successfully sent {msg_id}")
            sqs_client.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg_receipt)
        else:
            # otherwise we will handle the error. and log the error into s3
            logger.error(f"Failed to send {hierarchy}: {msg_id} to WD, uploading error to S3")
            write_error_s3(error_bucket, event, context, msg, response)

            # send to error state if this is a hierarchy and no_fails is not enabled
            if hierarchy in stop_when_fail and no_fails != "True":
                raise Exception("error in hierarchy. logged in s3")

    # update queue length including in-flight for the ones unprocessed on this run
    queue_length = sqs_get_queue_length(sqs_client, sqs_url, "total")

    return {
        "hierarchy": hierarchy,
        "deltas_length": event["deltas_length"],
        "queue_length": int(queue_length),
        "date": date,
        "mode": event["mode"],
    }


def process_site(event, context, msg, zeep_client, zeep_service, version, error_bucket, no_fails):
    """
    This is processing method for sites:
        1. Sends a Get_Location call to WD for this site ID
        2. If the site doesn't exist in WD, set default Time_Profile_ID and add
        3. If this site exists, check if it's active in workday - if not active skip
        4. Get the time profile if it exists for the site in WD
        5. If site exists and is active but doesn't have a time profile, set default (edge case)
    Then after all is true, and if a time profile is set, we validate the site address:
        1. Apply manual fixes e.g. Move Addr Line 1 -> Addr Line 2
        2. Check if all the required address fields exist
        3. Remove non-required address fields
        
    Returns True if we want to add the site to WD and sets the time_profile_id
    
    """

    msg_id = msg["body"]["id"]
    hierarchy = event["hierarchy"]

    # check if it's an active site coming out of g11
    if msg["body"]["status"] == "A":

        # check field validations for site, returns true if success, else error message
        try:
            file = event["local"]
        except KeyError:
            file = "process_deltas/addr_components.json"

        # validate the fields, validate_site returns error message if not valid. throw exception if invalid
        valid_site = validate_site(file, msg["body"])
        if not valid_site is True:
            # if the site data failed manual validations, then error to s3, mark processed, and throw exception
            write_error_s3(error_bucket, event, context, msg, valid_site)
            if no_fails == "False":
                raise Exception("error while validating site. logged in s3: " + valid_site)
            else:
                return False

        # then check if it is active in workday, and if it is, get the time_profile
        # if this site doesn't exist in WD, it will be created and auto-assigned 40 hours
        # if site is inactive in workday, then skip it
        time_profile = send_web_service(
            "GET_INACTIVE_SITE", msg_id, zeep_client, zeep_service, version,
        )
        if time_profile is None:
            logger.warning(f"{hierarchy} {msg_id} skipped because it is inactive in WD")
            return False
        else:
            # set the time profile for this site and send to Workday
            msg["body"]["time_profile_id"] = time_profile

        return True

    else:
        # delete from sqs because it is not active in G11
        logger.warning(f'{hierarchy} {msg_id} skipped because Status is "{msg["body"]["status"]}"')
        return False
