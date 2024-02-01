import json
import os
import time

import boto3

from shared.helper import logger


"""CONSTANTS"""

# Increased the visibility timeout - if all 10 fail in one batch within a Lambda, will get expired receipt handle error
# Ideally, in production, we won't have all 10 failing constantly and we can decrease this to speed things up
# SQS_VISIBILITY_TIMEOUT = 60
SQS_MAX_BATCH_SZ = 10
SQS_MAX_MSG_ID_SZ = 80
SQS_PURGE_TIME = 60
ATHENA_MAX_RESULTS_SZ = 1000  # Max query result for Named Queries
ATHENA_MAX_QUERY_SZ = 50  # Max rows that can be queried from a Table
HTTP_STATUS_OK = 200


def fetchall_athena(client, query_string, workgroup, db_name, s3_output):
    """
    Run a SQL query in Athena and paginate results to return a list of dicts
    https://gist.github.com/schledererj/b2e2a800998d61af2bbdd1cd50e08b76
    """

    # start execution of query in given db and workgroup
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": f"{db_name}"},
        ResultConfiguration={"OutputLocation": f"{s3_output}"},
        WorkGroup=workgroup,
    )["QueryExecutionId"]

    # poll the query and get paginated results once it completes
    query_status = None
    while query_status == "QUEUED" or query_status == "RUNNING" or query_status is None:
        query = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        query_status = query["Status"]["State"]
        if query_status == "FAILED" or query_status == "CANCELLED":
            # logger.error(query)
            raise Exception(query["Status"]["StateChangeReason"])
        time.sleep(10)
    results_paginator = client.get_paginator("get_query_results")
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id, PaginationConfig={"PageSize": ATHENA_MAX_QUERY_SZ}
    )

    # converts results into an array of dicts - since Lambdas pass data as JSON
    results = []
    column_names = None

    for results_page in results_iter:
        for row in results_page["ResultSet"]["Rows"]:
            column_values = [col.get("VarCharValue", None) for col in row["Data"]]
            if not column_names:
                column_names = column_values
            else:
                results.append(dict(zip(column_names, column_values)))

    return query_id, results


def get_record_count(athena, athena_wg, athena_db, s3_output, table):
    """get a count of the records in an Athena table"""

    # reduced_count_query = "SELECT COUNT(ID) FROM costcenter_new_run WHERE companycode IN (SELECT id FROM companycode_prev_run)"
    logger.info("Getting count of reduced cost centers")
    count_query = f"SELECT count(id) FROM {table}"

    query_id, count_results = fetchall_athena(athena, count_query, athena_wg, athena_db, s3_output)
    logger.info(f"Query ID: {query_id}")

    return int(count_results[0]["_col0"])


def sqs_send_message_batch(client, sqs_url, query_id, query_results, fifo=True):
    """Send all messages to SQS queue in groups of 10"""

    total_results = len(query_results)
    total_sent_msgs = 0

    # loop through all results in groups of 10
    for batch in range(0, total_results, SQS_MAX_BATCH_SZ):

        # loop throughgroups of 10 and create a message batch with 10 entries
        batch_entries = []
        group = query_results[batch : batch + SQS_MAX_BATCH_SZ]
        for index, result in enumerate(group):

            # create a unique id, max 80 characters
            msg_id = f"{batch+index:06}-{total_results:06}_{query_id}"

            # format w/ unique id, body, and put them in the same id
            # this id isn't that important, because content based deuplication id is generated based on body
            entry = {
                "Id": msg_id[: SQS_MAX_MSG_ID_SZ - 1],
                "MessageBody": json.dumps(result),
            }

            # messageGroupId is required for FIFO queues, and can't be used on standard queues
            if fifo:
                entry["MessageGroupId"] = query_id

            batch_entries.append(entry)

        # send batch of 10 messages to SQS
        response = client.send_message_batch(QueueUrl=sqs_url, Entries=batch_entries)

        # check if we have sent all of the messages in this batch
        if response["ResponseMetadata"]["HTTPStatusCode"] != HTTP_STATUS_OK:
            raise Exception("Failed to execute send_message_batch()")
        else:
            expected_msgs = len(group)
            try:
                sent_msgs = len(response["Successful"])
                if expected_msgs != sent_msgs:
                    raise Exception("Failed to send these messages: " + response["Failed"])
                else:
                    total_sent_msgs += sent_msgs
            except KeyError:
                raise Exception("Failed to send all messages")

    return total_sent_msgs


def sqs_get_queue_length(client, sqs_url, value="visible"):
    """get the length of the SQS queue, this is imprecise because messages can be in flight"""

    # need to add delay or else it doesn't get accurate values, 2 seems okay
    # note that these metrics may not achieve consitency until >= 1 min after producers stop
    time.sleep(2)

    attributes = [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible",
    ]

    # send request to get these attributes
    response = client.get_queue_attributes(QueueUrl=sqs_url, AttributeNames=attributes)

    http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if http_status_code != HTTP_STATUS_OK:
        raise Exception("Failed send get_queue_attributes()")

    # we should never have delayed messages, but the other ones should be factored in
    num_msgs = response["Attributes"]["ApproximateNumberOfMessages"]
    num_msgs_delayed = response["Attributes"]["ApproximateNumberOfMessagesDelayed"]
    num_msgs_not_vis = response["Attributes"]["ApproximateNumberOfMessagesNotVisible"]

    # return the queue attribute requested
    if value == "visible":
        return int(num_msgs)
    elif value == "inflight":
        return int(num_msgs_not_vis)
    elif value == "delayed":
        return int(num_msgs_delayed)
    elif value == "total":
        return int(num_msgs) + int(num_msgs_delayed) + int(num_msgs_not_vis)
    else:
        return {
            "visible": int(num_msgs),
            "inflight": int(num_msgs_not_vis),
            "delayed": int(num_msgs_delayed),
        }


def sqs_receive_message_batch(client, sqs_url):
    """
    Get up to 10 messages from SQS queue. First check if any in-flight messages, and wait if there are.
    This is because messages with the same message group id, if left unprocessed without DLQ will throw
    error on next receive message. This is how SQS maintains FIFO in message in the same group.
    
    Returns a dict with:
        body: the message body containing a dict w/ the data that needs to be sent
        delete:
            ReceiptHandle - used to delete a single message from the queue
            Id - MessageId used w/ ReceiptHandle when deleting batch of messages from the queue
    """

    # add retry because need to factor in visibility timeout and inflight messages
    waiting = 0
    while waiting < 6:
        # get number of inflight messages (returning to queue to be processed, or going to DLQ)
        msgs_inflight = sqs_get_queue_length(client, sqs_url, "inflight")

        # if there are none in-flight, then we can now receive
        if msgs_inflight == 0:
            break
        else:
            logger.warning(
                f"Waiting for {msgs_inflight} messages still in flight. Attempt number {waiting}"
            )
            time.sleep(10)
            waiting += 1
    else:
        raise Exception("Failed to get messages, semething stuck in flight")

    # if no more messages in queue, and no more inflight messages, then can't receive
    # we should never get to this empty queue step because step function should end by then
    if sqs_get_queue_length(client, sqs_url, "visible") == 0:
        raise Exception("Queue is empty")
    else:
        # get a batch of messages from SQS
        response = client.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=["SequenceNumber"],
            MaxNumberOfMessages=SQS_MAX_BATCH_SZ,
            # VisibilityTimeout=SQS_VISIBILITY_TIMEOUT,  # set to lower for testing
        )

    # check if we got any messages in the response
    messages = []

    if response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_STATUS_OK:
        try:
            msg_batch = response["Messages"]

            # get what we need out of the messages from the queue
            if len(msg_batch) > 0:
                for msg in msg_batch:
                    # delete message batch needs both these values. delete message only needs receipt handle
                    msg_id = msg["MessageId"]
                    msg_receipt = msg["ReceiptHandle"]

                    # body - contains the data for webservice (row of data from SAP/RDS)
                    msg_body = json.loads(msg["Body"])

                    # combine both into response for use in Lambda
                    messages.append(
                        {"body": msg_body, "delete": {"Id": msg_id, "ReceiptHandle": msg_receipt},}
                    )
        except KeyError:
            logger.warning("Receive message batch successful, but contains no messages")

    return messages


def sqs_delete_message_batch(client, sqs_url, delete):
    """delete a batch of messages from the queue"""

    num_msgs = len(delete)

    if 0 < num_msgs <= SQS_MAX_BATCH_SZ:
        response = client.delete_message_batch(QueueUrl=sqs_url, Entries=delete)

        # check if delete succeeded
        if response["ResponseMetadata"]["HTTPStatusCode"] != HTTP_STATUS_OK:
            # logger.error(response)
            raise Exception("Failed to send request to delete messages")

        # if any of the receipt handles expired, they could not be deleted and went back to the queue.
        if "The receipt handle has expired" in json.dumps(response):
            raise Exception("Failed to mark processed, RAISE VISIBILITY TIMEOUT")

        # check if we successfully deleted all the messages
        try:
            if len(response["Successful"]) != num_msgs:
                failed = response["Failed"]
                # logger.error(failed)
                raise Exception(f"Failed to delete {len(failed)} messages from SQS")
        except KeyError:
            raise Exception("Failed to delete all messages received from SQS")

    return response


def sqs_purge_queue(client, sqs_url, delay=SQS_PURGE_TIME):
    """Purges a queue - will fail if queue is already being purged"""
    response = client.purge_queue(QueueUrl=sqs_url)

    # recommended to wait 60 seconds for message deletion
    time.sleep(delay)

    return response
