import pytest
import json
import os
import random
import mock
from unittest.mock import patch
import boto3
from process_deltas import helper_boto3

# generate some fake results from Athena
test_file_dir = "stack/process_deltas/tests/tmp/"
results = [{"id": random.randint(0, 9), "data": random.choice("abcdefg")} for _ in range(11)]
difference = 5


@pytest.mark.dependency()
def test_upload_file(aws_credentials, s3_data_bucket):
    from shared.helper import upload_file

    # check /tmp directory exists
    if not os.path.exists(test_file_dir):
        os.makedirs(test_file_dir)

    # write a test file
    test_upload = test_file_dir + "test.txt"
    with open(test_upload, "w") as f:
        f.write("testing")

    # upload file to s3
    bucket = os.environ.get("BUCKET")
    prefix = os.environ.get("COSTCENTER_NEW")
    filename = "test.txt"
    upload_file(test_upload, bucket, prefix, filename)

    # check that the file written to s3 is the same as local copy
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=f"{prefix}{filename}")
    with open(test_upload, "r") as f:
        assert f.read() == response["Body"].read().decode("utf-8")


@pytest.mark.dependency(depends=["test_upload_file"])
@patch("process_deltas.helper_boto3.fetchall_athena", return_value=("1234567", results))
@patch("process_deltas.helper_boto3.get_record_count", return_value=(len(results) + difference))
def test_lambda_handler_count(
    mock_get_record_count, mock_athena_results, aws_credentials, athena, sqs, s3_data_bucket
):
    """get the count of the reduced cost centers"""
    from process_deltas import process_cc
    from process_deltas.process_cc import lambda_handler

    event = {"hierarchy": "costcenter", "date": "2020-07-17", "mode": "continue"}

    process_cc.reduced_file_path = test_file_dir + "costcenter.json"
    response = lambda_handler(event, "")

    expected_response = {
        "hierarchy": "costcenter",
        "date": "2020-07-17",
        "mode": "continue",
        "reduced_count": difference,
    }

    assert response == expected_response


@pytest.mark.dependency(depends=["test_upload_file"])
@patch("process_deltas.helper_boto3.fetchall_athena", return_value=("1234567", results))
@patch("process_deltas.helper_boto3.get_record_count", return_value=(len(results) + difference))
def test_lambda_handler_s3(
    mock_get_record_count, mock_athena_results, aws_credentials, athena, s3_data_bucket
):
    """check that the data read from the reduction athena query is written to reduced_run/ prefix in s3"""
    from process_deltas import process_cc
    from process_deltas.process_cc import lambda_handler

    event = {"hierarchy": "costcenter", "date": "2020-07-17", "mode": "continue"}
    process_cc.reduced_file_path = test_file_dir + "costcenter.json"
    response = lambda_handler(event, "")
    assert response["reduced_count"] == difference

    # get the file that was uploaded to s3
    bucket = os.environ.get("BUCKET")
    prefix = os.environ.get("COSTCENTER_REDUCED")
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=f"{prefix}costcenter.json")
    rows = response["Body"].read().decode("utf-8").split("\n")

    # check that it is the same as the results received from fetchall_athena for the reduction query
    for i in range(len(results)):
        assert json.loads(rows[i]) == results[i]
