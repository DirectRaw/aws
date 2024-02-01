import pytest
import json
import os
import moto
from mock import patch
import boto3
from datetime import date


dt = date.today().strftime("%Y-%m-%d")


def get_event(hierarchy):
    """S3 Trigger Event to Lambda"""
    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "example-bucket",
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": "arn:aws:s3:::example-bucket",
                    },
                    "object": {
                        "key": f"{hierarchy}_new_run/{hierarchy}.json",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }


def get_execution(aws_credentials, step_function):
    """Gets the execution for the step function created in conftest"""
    client = boto3.client("stepfunctions")
    response = client.list_executions(stateMachineArn=os.environ.get("STEP_FUNC_ARN"))
    execution = response["executions"]
    return execution


def test_trigger_step_companycode(aws_credentials, step_function, s3_data_bucket):
    """Once get_companycode Lambda dumps file in companycode_new_run/, then onezeroone step function runs"""
    from process_deltas.trigger_step import lambda_handler

    event = get_event("companycode")
    lambda_handler(event, "")

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after site comes buildings
    assert f"onezeroone_{dt}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_trigger_step_costcenter(aws_credentials, step_function, s3_data_bucket):
    """Once get_costcenter Lambda dumps file in costcenter_new_run/, then ninetwoone step function runs"""
    from process_deltas.trigger_step import lambda_handler

    event = get_event("costcenter")
    lambda_handler(event, "")

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after site comes buildings
    assert f"ninetwosix_{dt}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


def test_trigger_step_building(aws_credentials, step_function, s3_data_bucket):
    """Once get_costcenter Lambda dumps file in costcenter_new_run/, then ninetwoone step function runs"""
    from process_deltas.trigger_step import lambda_handler

    event = get_event("building")
    lambda_handler(event, "")

    # get the execution status after
    execution = get_execution(aws_credentials, step_function)[0]

    # after site comes buildings
    assert f"ninetwoone_{dt}" in execution["name"]
    assert execution["status"] == "RUNNING" or execution["status"] == "SUCCEEDED"


@pytest.mark.parametrize(
    "hierarchy", ("ninetwosix", "onezeroone", "site", "ninetwoone")
)
def test_trigger_step_other_hierarchies(
    aws_credentials, step_function, s3_data_bucket, hierarchy
):
    """Other hierarchies will not trigger a step function"""
    from process_deltas.trigger_step import lambda_handler

    event = get_event(hierarchy)

    with pytest.raises(Exception) as exc_info:
        lambda_handler(event, "")

    assert str(exc_info.value) == "should never happen"


@pytest.mark.parametrize("hierarchy", ("companycode", "costcenter", "building"))
def test_trigger_step_manual(aws_credentials, step_function, s3_data_bucket, hierarchy):
    """Manually triggered lambda which dumps in s3 does not trigger a step function"""
    from process_deltas.trigger_step import lambda_handler

    event = get_event(f"MANUAL_TESTING_{hierarchy}")

    lambda_handler(event, "")

    assert len(get_execution(aws_credentials, step_function)) == 0

