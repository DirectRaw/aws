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
    """926 Cost Center Hierarchy"""

    # manual mode prevents s3 trigger
    try:
        mode = event["mode"]
        file_name = "MANUAL_TESTING_costcenter.json"
    except KeyError:
        file_name = "costcenter.json"

    # set local variables and file paths
    file_name = "ninetwosix.json"
    output_path = f"/tmp/{file_name}" if event else f"tmp/{file_name}"
    query_path = "queries/rds_926_cost_center_hierarchy.sql"
    if event:
        query_path = "get_data/" + query_path

    # get environment variables
    bucket_name = os.environ.get("BUCKET")
    bucket_prefix = os.environ.get("NINETWOSIX_NEW")

    # execute sql query for 926 and write to file
    get_rds(query_path, output_path)

    # upload to s3
    upload_file(output_path, bucket_name, bucket_prefix, file_name)


if __name__ == "__main__":
    with open("environ.json", "r") as f:
        environ = json.loads(f.read())
    os.environ["SECRETS"] = environ["SECRETS"]
    os.environ["BUCKET"] = environ["BUCKET"]
    os.environ["NINETWOSIX_NEW"] = environ["NINETWOSIX_NEW"]
    lambda_handler("", "")
