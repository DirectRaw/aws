try:
    import unzip_requirements
except ImportError:
    pass

import json
import os

import boto3

try:
    from process_deltas.helper_boto3 import fetchall_athena, get_record_count
    from shared.helper import logger, upload_file
except ModuleNotFoundError:
    from helper_boto3 import fetchall_athena, get_record_count
    import sys

    sys.path.append("../")
    from shared.helper import logger, upload_file


reduced_file_path = "/tmp/costcenter.json"


def lambda_handler(event, context):
    """Lambda handler that runs ONLY for cost center step function, before process_deltas"""

    # get event variables - this is passed by the Lambda triggered by S3 event which executes the Step Func
    hierarchy = event["hierarchy"]

    # get environment variables - these are resources created in TF passed from SSM to serverless
    bucket = os.environ.get("BUCKET")
    s3_output = os.environ.get("ATHENA_OUTPUT")
    athena_db = os.environ.get("ATHENA_DB")
    athena_wg = os.environ.get("ATHENA_WG")

    # only used for cost center
    new_prefix = os.environ.get("COSTCENTER_NEW")
    reduced_prefix = os.environ.get("COSTCENTER_REDUCED")
    query_id = os.environ.get("COSTCENTER_REDUCED_QUERY")

    # get the Named Query for reduced cost centers (this query selects from cost_center_new table)
    athena = boto3.client("athena")
    sql_query = athena.get_named_query(NamedQueryId=query_id)["NamedQuery"]["QueryString"]

    # execute the SQL query in Athena, returns a list of cost centers less those with inactive company codes
    query_id, reduced_list = fetchall_athena(athena, sql_query, athena_wg, athena_db, s3_output)
    logger.warning(f"Reduced costcenter QueryExecutionId: {query_id}")

    # write it locally to lambda /tmp directory
    with open(reduced_file_path, "w") as f:
        for row in reduced_list:
            f.write(json.dumps(row))
            f.write("\n")

    # upload it to data_bucket/costcenter_reduced_run/
    upload_file(reduced_file_path, bucket, reduced_prefix, "costcenter.json")

    # get a count of # of reductions for comparison
    new_run_count = get_record_count(athena, athena_wg, athena_db, s3_output, new_prefix[:-1])
    reduced_count = new_run_count - len(reduced_list)
    logger.warning(
        f"Removed {reduced_count} costcenters (new_run: {new_run_count}, reduced: {len(reduced_list)})"
    )

    # return values to step function
    return {
        "hierarchy": hierarchy,
        "date": event["date"],
        "mode": event["mode"],
        "reduced_count": reduced_count,
    }
