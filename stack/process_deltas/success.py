try:
    import unzip_requirements
except ImportError:
    pass

import os
from datetime import datetime

import boto3
import botocore

from shared.helper import logger


def lambda_handler(event, context):
    """
    Successfully completed Step Function, move file from new_run/ prefix for hierachy to old run.
    Also trigger any subsequent Step Functions / Lambda Functions
    """

    # get step function variables
    hierarchy = event["hierarchy"]
    date = event["date"]
    mode = event["mode"]

    # get environment variables
    main_bucket = os.environ.get("BUCKET")
    backup_bucket = os.environ.get("BACKUP_BUCKET")
    new_prefix = os.environ.get(f"{hierarchy.upper()}_NEW")
    prev_prefix = os.environ.get(f"{hierarchy.upper()}_OLD")
    new_key = f"{new_prefix}{hierarchy}.json"
    prev_key = f"{prev_prefix}{hierarchy}.json"

    # only needed for cost center
    reduced_prefix = os.environ.get("COSTCENTER_REDUCED")
    reduced_key = f"{reduced_prefix}{hierarchy}.json"

    s3_client = boto3.client("s3")

    # backup files in new_run, prev_run, and reduced_run
    backup_files(
        hierarchy,
        date,
        s3_client,
        main_bucket,
        backup_bucket,
        new_prefix,
        prev_prefix,
        reduced_prefix,
    )

    # manual testing for debugging, don't delete files after success
    if mode != "continue":
        return

    # store the new_run/ as the prev_run/ for next delta comparison
    # except cost center: reduced_run/costcenter.json --> prev_run/costcenter.json
    response = copy_object(
        s3_client=s3_client,
        source_bucket=main_bucket,
        source_key=(reduced_key if hierarchy == "costcenter" else new_key),
        dest_bucket=main_bucket,
        dest_key=prev_key,
    )

    # if the file was successfully moved, then we can delete the file
    try:
        result = response["CopyObjectResult"]
    except KeyError as e:
        logger.error(e)
        raise Exception("File was not copied successfully. Skipping deletion.")

    # delete new_run after moving it to prev_run, except for onezeroone, which is required by companycode
    if result and hierarchy != "onezeroone":
        s3_client.delete_object(Bucket=main_bucket, Key=new_key)

        if hierarchy == "costcenter":
            # for cost center delete reduced_run/ and new_run/
            s3_client.delete_object(Bucket=main_bucket, Key=reduced_key)

    # so delete onezeroone during the companycode run
    if result and hierarchy == "companycode":
        s3_client.delete_object(Bucket=main_bucket, Key="onezeroone_new_run/onezeroone.json")

    # if step function not in debug mode, then execute following step function
    execute_next(hierarchy, date, mode)


def backup_files(
    hierarchy, date, s3_client, main_bucket, backup_bucket, new_prefix, prev_prefix, reduced_prefix
):
    """ backup all files used in the run """
    new_key = f"{new_prefix}{hierarchy}.json"
    prev_key = f"{prev_prefix}{hierarchy}.json"
    reduced_key = f"{reduced_prefix}{hierarchy}.json"

    # copy new_run/<hierarchy>.json --> backup/<hierarchy>_new_run.json
    response = copy_object(
        s3_client=s3_client,
        source_bucket=main_bucket,
        source_key=new_key,
        dest_bucket=backup_bucket,
        dest_key=f"{date}/{new_prefix[:-1]}.json",
    )

    # copy prev_run/<hierarchy>.json --> backup/<hierarchy>_prev_run.json
    response = copy_object(
        s3_client=s3_client,
        source_bucket=main_bucket,
        source_key=prev_key,
        dest_bucket=backup_bucket,
        dest_key=f"{date}/{prev_prefix[:-1]}.json",
    )

    # copy reduced_run/costcenter.json --> backup/costcenter_reduced_run.json
    if hierarchy == "costcenter":
        response = copy_object(
            s3_client=s3_client,
            source_bucket=main_bucket,
            source_key=reduced_key,
            dest_bucket=backup_bucket,
            dest_key=f"{date}/{reduced_prefix[:-1]}.json",
        )


def copy_object(s3_client, source_bucket, source_key, dest_bucket, dest_key):
    """Copy between s3 buckets, if source doesn't exist, copy blank to destination"""
    logger.info(f"Copying {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")

    # check if the source file that we are trying to copy exists
    try:
        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
    except botocore.exceptions.ClientError:
        logger.error(f"Source {source_bucket}/{source_key} does not exist")

        # if source doesn't exist, put empty key for placeholder in destination
        response = s3_client.put_object(Bucket=dest_bucket, Key=dest_key)
        return response

    # if it does exist, move it to destination bucket
    try:
        response = s3_client.copy_object(
            ACL="bucket-owner-read",
            Bucket=dest_bucket,
            ServerSideEncryption="AES256",
            Key=dest_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )

        return response
    except Exception as e:
        logger.error(str(e))
        raise e


def execute_next(hierarchy, date, mode):
    """
    Execute any next steps after a step function completes:
    
    onezeroone -> companycode -> lambdas: get_ninetwoone and get_ninetwosix in shared account
    ninetwosix -> costcenter
    ninetwoone -> site -> building
    """
    statemachine_arn = os.environ.get("STEP_FUNC_ARN")

    if hierarchy == "companycode":
        # after completion of company code step function, execute get_921 and get_926 lambdas
        lambda_client = boto3.client("lambda")
        ninetwosix_arn = os.environ.get("GET_NINETWOSIX_ARN")
        response_926 = lambda_client.invoke(FunctionName=ninetwosix_arn, InvocationType="Event")
        ninetwoone_arn = os.environ.get("GET_NINETWOONE_ARN")
        response_921 = lambda_client.invoke(FunctionName=ninetwoone_arn, InvocationType="Event")
        next_hierarchy = False
    elif hierarchy == "onezeroone":
        # after completion of 101 step function, execute companycode step function
        next_hierarchy = "companycode"
    elif hierarchy == "ninetwosix":
        # after completion of 926 step function, exectute costcenter step function
        next_hierarchy = "costcenter"
    elif hierarchy == "ninetwoone":
        # after completion of 921 step function, execute site step function
        next_hierarchy = "site"
    elif hierarchy == "site":
        # after completion of stie step function, execute building step function
        next_hierarchy = "building"
    else:
        return

    if next_hierarchy:
        # setup payload and execution name
        payload = '{"hierarchy" : "%s", "date": "%s", "mode": "%s"}' % (
            next_hierarchy,
            date,
            mode,
        )
        time = datetime.now()
        name = f"{next_hierarchy}_{date}_{time.hour}-{time.minute}-{time.second}"

        # execute the step function
        step_client = boto3.client("stepfunctions")
        response = step_client.start_execution(
            stateMachineArn=statemachine_arn, name=name, input=payload
        )

        #  check if executed succesfully
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception("failed to invoke State Machine")
        else:
            logger.info(f"triggered step function for {next_hierarchy}")
    else:
        return
