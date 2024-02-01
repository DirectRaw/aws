import pytest
import json
import os
import random
import mock
from mock import patch
import boto3


def test_fetchall_athena(aws_credentials, athena):
    from process_deltas.helper_boto3 import fetchall_athena

    # client = boto3.client("athena")
    # workgroup = os.environ.get("ATHENA_WG")
    # database = os.environ.get("ATHENA_DB")
    # query = "SELECT * from table1"
    # results = fetchall_athena(
    #     client, query, workgroup, database, "s3://bucket-name/prefix/"
    # )
    # @TODO 07/20/20 - mock_athena not developed yet

    assert 2 == 2


@patch("process_deltas.helper_boto3.fetchall_athena", return_value=("123", [{"_col0": "86725"}]))
def test_get_record_count(mock_fetchall_athena, aws_credentials, athena):
    """testing getting count of records in a table using fetchall_athena"""
    from process_deltas.helper_boto3 import get_record_count

    client = boto3.client("athena")
    athena_wg = os.environ.get("ATHENA_WG")
    athena_db = os.environ.get("ATHENA_DB")
    s3_output = os.environ.get("ATHENA_OUTPUT")
    table = "random table"

    results = get_record_count(client, athena_wg, athena_db, s3_output, table)

    assert results == 86725


def test_sqs_send_message_batch(aws_credentials, sqs_fifo):
    from process_deltas.helper_boto3 import sqs_send_message_batch, sqs_get_queue_length

    client = boto3.client("sqs")
    sqs_url = os.environ.get("ONEZEROONE_SQS_URL")

    # generate some random messages and send them all to SQS in batches of 10
    query_id = "123456789"
    query_results = [{"data": random.choice("abcdefghijklmnopqrstuvwxyz")} for _ in range(10)]
    num_messages_sent = sqs_send_message_batch(client, sqs_url, query_id, query_results)

    assert num_messages_sent == len(query_results)


def test_sqs_receive_message_batch(aws_credentials, sqs_fifo):
    from process_deltas.helper_boto3 import (
        sqs_send_message_batch,
        sqs_get_queue_length,
        sqs_receive_message_batch,
        sqs_delete_message_batch,
    )

    client = boto3.client("sqs")
    sqs_url = os.environ.get("ONEZEROONE_SQS_URL")

    # load 20 items into SQS
    messages_sent = [{"data": random.choice("abcdefghijklmnopqrstuvwxyz")} for _ in range(20)]
    sqs_send_message_batch(client, sqs_url, "123456789", messages_sent)

    # receveive a group of 10 messages
    messages_received = sqs_receive_message_batch(client, sqs_url)
    assert messages_received is not None
    assert sqs_get_queue_length(client, sqs_url, value="inflight") == 10

    # check that they are the messages we sent, then mark processed by deleting individually
    for msg in messages_received:
        assert msg["body"] in messages_sent
        client.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg["delete"]["ReceiptHandle"])

    # check that all the inflight messages have been processed
    assert sqs_get_queue_length(client, sqs_url, "inflight") == 0

    # receveive another group of 10
    messages_received = sqs_receive_message_batch(client, sqs_url)
    assert messages_received is not None

    # check that they are the messages we sent, then mark processed
    delete = []
    for msg in messages_received:
        assert msg["body"] in messages_sent
        delete.append(msg["delete"])

    # delete a batch of 10 messages
    response = sqs_delete_message_batch(client, sqs_url, delete)
    assert len(response["Successful"]) == len(delete)


def test_sqs_get_queue_length(aws_credentials, sqs_fifo):
    from process_deltas.helper_boto3 import (
        sqs_send_message_batch,
        sqs_get_queue_length,
        sqs_receive_message_batch,
        sqs_purge_queue,
    )

    client = boto3.client("sqs")
    sqs_url = os.environ.get("ONEZEROONE_SQS_URL")

    # load 200 item into SQS
    for _ in range(20):
        test_sqs_send_message_batch(aws_credentials, sqs_get_queue_length)
    assert sqs_get_queue_length(client, sqs_url, value="total") == 200

    # receive 10 messages
    assert sqs_receive_message_batch(client, sqs_url) is not None

    # check that there are 10 messages being processed
    assert sqs_get_queue_length(client, sqs_url, value="inflight") == 10
    assert sqs_get_queue_length(client, sqs_url, value="visible") == 190
    assert sqs_get_queue_length(client, sqs_url, value="delayed") == 0

    # purge the queue
    sqs_purge_queue(client, sqs_url, 0)
    assert sqs_get_queue_length(client, sqs_url, value="total") == 0
