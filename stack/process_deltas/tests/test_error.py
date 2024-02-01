import pytest
import json
import os
from moto import mock_s3
from mock import patch
import boto3

from process_deltas.tests.data import context


def test_error_handled(aws_credentials, s3_error_bucket):
    """if error was already written s3, it shouldn't be written in error step again"""
    from process_deltas.error import lambda_handler

    event = {
        "hierarchy": "onezeroone",
        "date": "2020-07-12",
        "deltas_length": "300",
        "queue_length": "400",
        "error": {"Cause": '{"errorMessage": "error in hierarchy. logged in s3"}'},
    }

    lambda_handler(event, context)

    error_bucket = os.environ.get("ERROR_BUCKET")
    conn = boto3.client("s3")
    objects = conn.list_objects(Bucket=error_bucket)
    assert not "Contents" in objects


def test_error_runtime1(aws_credentials, s3_error_bucket):
    from process_deltas.error import lambda_handler

    event = {
        "hierarchy": "onezeroone",
        "date": "2020-07-12",
        "deltas_length": "300",
        "queue_length": "400",
        "error": {"Cause": '{"errorMessage": "ERROR"}'},
    }

    lambda_handler(event, context)

    error_bucket = os.environ.get("ERROR_BUCKET")
    conn = boto3.client("s3")
    objects = conn.list_objects(Bucket=error_bucket)
    assert "2020-07-12/onezeroone/2020-07-12_onezeroone_error_" in objects["Contents"][0]["Key"]


def test_error_runtime2(aws_credentials, s3_error_bucket):
    import boto3
    from process_deltas.error import lambda_handler

    event = {
        "hierarchy": "onezeroone",
        "date": "2020-07-12",
        "queue_length": "400",
        "error": {"Cause": '{"errorMessage": "ERROR"}'},
    }

    lambda_handler(event, context)

    error_bucket = os.environ.get("ERROR_BUCKET")
    conn = boto3.client("s3")
    objects = conn.list_objects(Bucket=error_bucket)
    assert "2020-07-12/onezeroone/2020-07-12_onezeroone_error_" in objects["Contents"][0]["Key"]


def test_error_runtime3(aws_credentials, s3_error_bucket):
    import boto3
    from process_deltas.error import lambda_handler

    event = {
        "hierarchy": "onezeroone",
        "date": "2020-07-12",
        "error": {"Cause": '{"errorMessage": "ERROR"}'},
    }

    lambda_handler(event, context)

    error_bucket = os.environ.get("ERROR_BUCKET")
    conn = boto3.client("s3")
    objects = conn.list_objects(Bucket=error_bucket)
    assert "2020-07-12/onezeroone/2020-07-12_onezeroone_error_" in objects["Contents"][0]["Key"]
