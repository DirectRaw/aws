import pytest
import json
import os
import random
import mock
from unittest.mock import patch
import boto3
from process_deltas import helper_boto3

# generate some fake results from Athena
query_results = [{"id": random.randint(0, 9), "data": random.choice("abcdefg")} for _ in range(11)]


@patch("process_deltas.helper_boto3.fetchall_athena", return_value=("1234567", query_results))
@patch("process_deltas.helper_boto3.sqs_purge_queue", return_value=True)
def test_lambda_handler_results(
    mock_sqs_purge_queue, mock_athena_results, aws_credentials, athena, sqs_fifo,
):
    """Get deltas from Athena then add them to SQS"""

    from process_deltas.process_deltas import lambda_handler

    event = {"hierarchy": "onezeroone", "date": "2020-07-17", "mode": "continue"}

    response = lambda_handler(event, "")

    expected_response = {
        "hierarchy": "onezeroone",
        "deltas_length": 11,
        "queue_length": 11,
        "date": "2020-07-17",
        "mode": "continue",
    }

    assert response == expected_response


# the patched function seems to overwrite ALL test cases

# @patch("process_deltas.helper_boto3.fetchall_athena", return_value=("1234567", []))
# @patch("process_deltas.helper_boto3.fetch_delta_named_query", return_value="SELECT")
# @patch("process_deltas.helper_boto3.sqs_purge_queue", return_value=True)
# def test_lambda_handler_noresults(
#     mock_sqs_purge_queue,
#     mock_fetch_delta_named_query,
#     mock_athena_results,
#     aws_credentials,
#     athena,
#     sqs_fifo,
# ):
#     """No results from athena, succeed but with no queue length or deltas length"""

#     from process_deltas.process_deltas import lambda_handler

#     event = {"hierarchy": "onezeroone", "date": "2020-07-17", "mode": "continue"}

#     response = lambda_handler(event, "")

#     expected_response = {
#         "hierarchy": "onezeroone",
#         "deltas_length": 0,
#         "queue_length": 0,
#         "date": "2020-07-17",
#         "mode": "continue",
#     }

#     assert response == expected_response
