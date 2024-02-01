import pytest
import json
import os
import moto
from moto import mock_lambda
from mock import patch
import boto3

date = "2020-07-17"
mode = "continue"


def get_execution(aws_credentials, step_function):
    """Gets the execution for the step function created in conftest"""
    client = boto3.client("stepfunctions")
    response = client.list_executions(stateMachineArn=os.environ.get("STEP_FUNC_ARN"))
    execution = response["executions"]
    return execution


"""Test copy_object"""


def test_copy_object_exists(aws_credentials, s3_data_bucket):
    """copies an object from source to destination"""
    from process_deltas.success import copy_object

    client = boto3.client("s3")
    source_bucket = os.environ.get("BUCKET")
    dest_bucket = os.environ.get("BACKUP_BUCKET")

    # get the first file from the source s3 bucket
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    file = keys[0]

    # copy and check it was successfully copied
    response = copy_object(client, source_bucket, file, dest_bucket, f"moved_{file}")
    assert "CopyObjectResult" in response

    # check that it was successfully copied to dest bucket
    response = client.head_object(Bucket=dest_bucket, Key=f"moved_{file}")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_copy_object_nonexistent(aws_credentials, s3_data_bucket):
    """copies an object from source to destination"""
    from process_deltas.success import copy_object

    client = boto3.client("s3")
    source_bucket = os.environ.get("BUCKET")
    dest_bucket = os.environ.get("BACKUP_BUCKET")

    # file which doesn't exist in source bucket
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    file = "imaginary_file.json"
    assert not file in keys

    # run copy function, which will create a empty prefix for nonexistent source file
    response = copy_object(client, source_bucket, file, dest_bucket, f"moved_{file}")
    assert not "CopyObjectResult" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # check that it was successfully copied to dest bucket
    response = client.head_object(Bucket=dest_bucket, Key=f"moved_{file}")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


"""Test execute_next() in success.py"""


def test_success_execute_next_onezeroone(aws_credentials, step_function):
    from process_deltas.success import execute_next

    hierarchy = "onezeroone"
    execute_next(hierarchy, date, mode)

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after onezeroone hierarchy comes companycode step function
    assert f"companycode_{date}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_success_execute_next_ninetwosix(aws_credentials, step_function):
    from process_deltas.success import execute_next

    hierarchy = "ninetwosix"
    execute_next(hierarchy, date, mode)

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after ninetwosix hierarchy comes costcenters step function
    assert f"costcenter_{date}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_success_execute_next_ninetwoone(aws_credentials, step_function):
    from process_deltas.success import execute_next

    hierarchy = "ninetwoone"
    execute_next(hierarchy, date, mode)

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after after ninetwoone hierarchy comes sites step function
    assert f"site_{date}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_success_execute_next_site(aws_credentials, step_function):
    from process_deltas.success import execute_next

    hierarchy = "site"
    execute_next(hierarchy, date, mode)

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after site comes buildings
    assert f"building_{date}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_success_execute_next_norun(aws_credentials, step_function, lambdas):
    from process_deltas.success import execute_next

    execute_next("costcenter", date, mode)
    execute_next("building", date, mode)
    execute_next("companycode", date, mode)

    # get the execution status after
    executions = get_execution(aws_credentials, step_function)

    # no step functions triggered after costcenter or building (they are the end)
    # companycode triggers lambda functinos instead of another step function
    assert len(executions) == 0


"""Test the lambda handler"""


def test_success_lambda_handler_success(aws_credentials, step_function, s3_data_bucket):
    from process_deltas.success import lambda_handler

    hierarchy = "building"

    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }

    lambda_handler(event, "")

    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 0

    # check that the file was moved successfully from new run to prev run prefix
    client = boto3.client("s3")
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    assert f"{hierarchy}_prev_run/{hierarchy}.json" in keys
    assert f"{hierarchy}_new_run/{hierarchy}.json" not in keys

    # check that both prev run and new run were backed up
    responses = client.list_objects(Bucket=os.environ.get("BACKUP_BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    assert f"{event['date']}/{hierarchy}_prev_run.json" in keys
    assert f"{event['date']}/{hierarchy}_new_run.json" in keys


def test_success_lambda_handler_no_continue(aws_credentials, step_function, s3_data_bucket):
    """Debug mode, no futher step functions should be triggered. Files should not be moved either"""
    from process_deltas.success import lambda_handler

    hierarchy = "building"

    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": "don't continue manual testing",
        "deltas_length": 30,
    }

    lambda_handler(event, "")

    # check that the file were not moved
    client = boto3.client("s3")
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    assert f"{hierarchy}_prev_run/{hierarchy}.json" in keys
    assert f"{hierarchy}_new_run/{hierarchy}.json" in keys


def test_success_lambda_handler_exception(aws_credentials, step_function, s3_data_bucket):
    from process_deltas.success import lambda_handler

    event = {
        "hierarchy": "building",
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }

    # remove the s3 prefix for building_new_run, lambda will throw error because then cannot copy file from s3
    os.environ["BUILDING_NEW"] = ""

    with pytest.raises(Exception) as exc_info:
        lambda_handler(event, "")

    assert str(exc_info.value) == "File was not copied successfully. Skipping deletion."


def test_success_lambda_handler_exception_step(aws_credentials, step_function, s3_data_bucket):
    from process_deltas.success import lambda_handler

    event = {
        "hierarchy": "site",
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }

    # incorrect step function ARN
    os.environ["STEP_FUNC_ARN"] = "wrong arn"

    with pytest.raises(Exception) as exc_info:
        lambda_handler(event, "")

    assert (
        str(exc_info.value)
        == "An error occurred (InvalidArn) when calling the StartExecution operation: Invalid State Machine Arn: 'wrong arn'"
    )


def test_success_lambda_handler_onezeroone(aws_credentials, step_function, s3_data_bucket):
    """make sure we do not delete onezeroone until companycode"""
    from process_deltas.success import lambda_handler

    hierarchy = "onezeroone"
    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }
    lambda_handler(event, "")

    # onezeroone completion kicks off companycode
    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 1

    # check that the file was moved successfully from new run to prev run prefix
    client = boto3.client("s3")
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]

    # moved to prev_run/ but also not deleted from new_run/ yet
    assert f"{hierarchy}_prev_run/{hierarchy}.json" in keys
    assert f"{hierarchy}_new_run/{hierarchy}.json" in keys


def test_success_lambda_handler_companycode(
    aws_credentials, step_function, s3_data_bucket, lambdas
):
    """make sure we do not delete onezeroone until companycode"""
    from process_deltas.success import lambda_handler

    hierarchy = "companycode"
    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }
    lambda_handler(event, "")

    # company code kicks off 926 and 921 lambdas, not step function
    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 0

    # check that the file was moved successfully from new run to prev run prefix
    client = boto3.client("s3")
    responses = client.list_objects(Bucket=os.environ.get("BUCKET"))["Contents"]
    keys = [response["Key"] for response in responses]
    assert f"{hierarchy}_prev_run/{hierarchy}.json" in keys
    assert f"{hierarchy}_new_run/{hierarchy}.json" not in keys

    # also check that the onezeroone run is deleted at this point
    assert f"onezeroone_new_run/onezeroone.json" not in keys


def test_success_lambda_handler_costcenter_main_bucket(
    aws_credentials, step_function, s3_data_bucket
):
    """
    after successful run of cost center, only prev_run/ prefix should have a file 
    in the main data bucket, new_run/ and reduced_run/ should be empty
    """
    from process_deltas.success import lambda_handler

    # call lambda handler with cost center inputs
    hierarchy = "costcenter"
    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }
    lambda_handler(event, "")

    # cost center doesn't have any next executions
    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 0

    # set up s3 stuff
    client = boto3.client("s3")
    main_bucket = os.environ.get("BUCKET")

    # check that the file was moved successfully from new run to prev run prefix
    responses = client.list_objects(Bucket=main_bucket)["Contents"]
    main_bucket_keys = [response["Key"] for response in responses]

    # check that the appropriate files were deleted
    assert f"costcenter_prev_run/costcenter.json" in main_bucket_keys
    assert not f"costcenter_new_run/costcenter.json" in main_bucket_keys
    assert not f"costcenter_reduced_run/costcenter.json" in main_bucket_keys


def test_success_lambda_handler_costcenter_copied_contents(
    aws_credentials, step_function, s3_data_bucket
):
    """for cost center, after completion, the contents stored in prev_run/ in main bucket should be from reduced_run/, not new_run/"""
    from process_deltas.success import lambda_handler

    # call lambda handler with cost center inputs
    hierarchy = "costcenter"
    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }
    lambda_handler(event, "")

    # cost center doesn't have any next executions
    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 0

    # set up s3 stuff
    client = boto3.client("s3")
    main_bucket = os.environ.get("BUCKET")

    # check that the file stored to prev_run/ was from reduced_run/ - check conftest.py s3 initialization
    response = client.get_object(Bucket=main_bucket, Key="costcenter_prev_run/costcenter.json")
    assert json.loads(response["Body"].read())["source"] == "costcenter_reduced_run/"


def test_success_lambda_handler_costcenter_backup_bucket(
    aws_credentials, step_function, s3_data_bucket
):
    """all files including reduced_run/ copied to backup bucket for cost center"""
    from process_deltas.success import lambda_handler

    # call lambda handler with cost center inputs
    hierarchy = "costcenter"
    event = {
        "hierarchy": hierarchy,
        "date": date,
        "mode": mode,
        "deltas_length": 30,
    }
    lambda_handler(event, "")

    # cost center doesn't have any next executions
    executions = get_execution(aws_credentials, step_function)
    assert len(executions) == 0

    # set up s3 stuff
    client = boto3.client("s3")
    backup_bucket = os.environ.get("BACKUP_BUCKET")

    # check that all files were copied to backup, including reduced
    responses = client.list_objects(Bucket=backup_bucket)["Contents"]
    backup_bucket_keys = [response["Key"] for response in responses]
    assert f"{date}/costcenter_prev_run.json" in backup_bucket_keys
    assert f"{date}/costcenter_new_run.json" in backup_bucket_keys
    assert f"{date}/costcenter_reduced_run.json" in backup_bucket_keys
