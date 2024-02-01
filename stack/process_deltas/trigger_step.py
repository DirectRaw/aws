try:
    import unzip_requirements
except ImportError:
    pass
import os
from datetime import date, datetime

import boto3

from shared.helper import logger


def lambda_handler(event, context):
    """
    This Lambda is used to trigger Step Function. We need this because S3 Prefix
    cannot trigger a statemachine directly:
    - get_companycode from shared accoutn dumps data to companycode_new_run/ prefix
        this triggers the onezeroone hierarchy step function
    - get_costcenter from shared account dumps data to costcenter_new_run/ prefix
        this triggers the ninetwosix hierarchy step function
    - get_buildings from the workday account dumps data to building_new_run/ prefix
        this triggers the ninetwoone hierarchy step function
    """

    # get environment variables
    expected_bucket = os.environ.get("BUCKET")
    statemachine_arn = os.environ.get("STEP_FUNC_ARN")

    # get event variables
    try:
        s3_event = event["Records"][0]["s3"]
        key = s3_event["object"]["key"]
        bucket = s3_event["bucket"]["name"]
        prefix = key.split("/")[0]
        file = key.split("/")[1]
    except KeyError:
        # not coming from s3 trigger
        prefix = event["hierarchy"]

    # determine which hierarchy it is, and which state machine to use
    if "companycode" in prefix:
        hierarchy = "onezeroone"
    elif "costcenter" in prefix:
        hierarchy = "ninetwosix"
    elif "building" in prefix:
        hierarchy = "ninetwoone"
    else:
        raise Exception("should never happen")

    if "MANUAL_TESTING" in file:
        # if the s3 dump comes from a manually triggered test. then don't continue
        return

    mode = "continue"
    dt = date.today().strftime("%Y-%m-%d")
    input_config = '{"hierarchy" : "%s", "date": "%s", "mode" : "%s"}' % (hierarchy, dt, mode)

    # execute step function
    step_client = boto3.client("stepfunctions")
    time = datetime.now()
    name = f"{hierarchy}_{dt}_{time.hour}-{time.minute}-{time.second}"
    response = step_client.start_execution(
        stateMachineArn=statemachine_arn, name=name, input=input_config
    )

    # check if we were able to execute step function successfully
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise Exception("failed to invoke State Machine")
    else:
        logger.info(f"triggered step function for {hierarchy}")
