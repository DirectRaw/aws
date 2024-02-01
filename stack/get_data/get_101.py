try:
    import unzip_requirements
except ImportError:
    pass

import os
import json

try:
    from get_data.helper_get import get_rds, upload_file
except ModuleNotFoundError:
    from helper_get import get_rds, upload_file


def lambda_handler(event, context):

    # set local variables and file paths
    file_name = "onezeroone.json"
    output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"
    query_path = "queries/rds_101_legal_entities.sql"
    if event:
        query_path = "get_data/" + query_path

    # get environment variables
    bucket_name = os.environ.get("BUCKET")
    bucket_prefix = os.environ.get("ONEZEROONE_NEW")

    # execute sql query for 101 and write to file
    get_rds(query_path, output_path)

    # upload to s3
    upload_file(output_path, bucket_name, bucket_prefix, file_name)


if __name__ == "__main__":
    with open("environ.json", "r") as f:
        environ = json.loads(f.read())
    os.environ["SECRETS"] = environ["SECRETS"]
    os.environ["BUCKET"] = environ["BUCKET"]
    os.environ["ONEZEROONE_NEW"] = environ["ONEZEROONE_NEW"]
    lambda_handler("", "")
