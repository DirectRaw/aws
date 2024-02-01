try:
    import unzip_requirements
except ImportError:
    pass

import os

import boto3

from process_deltas.helper_boto3 import (
    fetchall_athena,
    sqs_get_queue_length,
    sqs_purge_queue,
    sqs_send_message_batch,
)
from shared.helper import logger


def lambda_handler(event, context):
    """
    Lambda handler that runs as the first state of the Step Function. Executes a delta comparison
    SQL query in Athesqs_fifona for a hierarchy and sends deltas to SQS for Step Function to process
    """

    # get event variables - this is passed by the Lambda triggered by S3 event which executes the Step Func
    hierarchy = event["hierarchy"]

    # get environment variables - these are resources created in TF passed from SSM to serverless
    s3_output = os.environ.get("ATHENA_OUTPUT")
    athena_db = os.environ.get("ATHENA_DB")
    athena_wg = os.environ.get("ATHENA_WG")
    sqs_url = os.environ.get(f"{hierarchy.upper()}_SQS_URL")
    query_id = os.environ.get(f"{hierarchy.upper()}_QUERY")

    # instantiate boto3 clients
    athena = boto3.client("athena")
    sqs_client = boto3.client("sqs")

    #  purge the queue, this has 60 second delay
    sqs_purge_queue(sqs_client, sqs_url)

    # get the Named Query for comparing deltas from a specified Workgroup in Athena
    sql_query = athena.get_named_query(NamedQueryId=query_id)["NamedQuery"]["QueryString"]

    # execute the SQL query in Athena
    query_id, delta_list = fetchall_athena(athena, sql_query, athena_wg, athena_db, s3_output)
    logger.warning(f"{hierarchy} process_delta QueryExecutionId: {query_id}")

    # get then umber of deltas
    num_deltas = len(delta_list)
    logger.warning(f"There are {num_deltas} deltas for {hierarchy}")

    # send deltas to SQS FIFO queue if there are any deltas
    if num_deltas > 0:

        # print list of delta ids for debugging
        delta_list_ids = [delta["id"] for delta in delta_list]
        logger.warning(f"{hierarchy} ids w/ changes: " + ", ".join(map(str, delta_list_ids)))

        # costcenters use regular queue and fifo queue
        fifo = False if hierarchy == "costcenter" else True
        num_sent = sqs_send_message_batch(sqs_client, sqs_url, query_id, delta_list, fifo)
        num_sqs = sqs_get_queue_length(sqs_client, sqs_url, "total")

        # check that we have successfully sent all the deltas to the queue
        # ignore for non-fifo SQS (costcenter), since the messages begin processing as soon as queue receives
        if fifo and not (num_deltas == num_sent == num_sqs):
            logger.error(f"Messages (deltas) from Athena query: {num_deltas}")
            logger.error(f"Messages successfully sent to SQS: {num_sent}")
            logger.error(f"Messages in SQS: {num_sqs}")
            logger.error(f"Error getting from Athena/loading to SQS for: {hierarchy}")
            raise Exception("Failed to send the correct number of deltas to the queue")

    else:
        # if there are no deltas, we will send set deltas_length to 0, and the step function will complete
        num_deltas = 0
        num_sqs = 0

    # return values to step funciton
    return {
        "hierarchy": hierarchy,
        "deltas_length": int(num_deltas),
        "queue_length": int(num_sqs),
        "date": event["date"],
        "mode": event["mode"],
    }
