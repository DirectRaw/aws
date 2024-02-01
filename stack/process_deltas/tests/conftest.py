# from __future__ import unicode_literals

import os
import io
import pytest
import moto
from moto import (
    mock_secretsmanager,
    mock_sqs,
    mock_athena,
    mock_s3,
    mock_stepfunctions,
    mock_sts,
    mock_lambda,
    mock_iam,
)
from mock import patch
import boto3
import json
import sure  # noqa
import zipfile
from botocore.exceptions import ClientError
from process_deltas.tests.data import HIERARCHIES


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture(request):
    patched = patch.dict(
        os.environ,
        {"AWS_DEFAULT_REGION": "us-east-1", "TF_WORKSPACE": "test", "NO_FAILS": "False",},
    )
    patched.__enter__()

    def unpatch():
        patched.__exit__()

    request.addfinalizer(unpatch)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def secrets_manager(aws_credentials):
    with mock_secretsmanager():
        secrets = {
            "wd_client_cert": "-----BEGIN CERTIFICATE-----\nMIIEqjCCA5KgAwIBAgIJALOCBen0S+W5MA0GCSqGSIb3DQEBBQUAMIGUMQswCQYD\nVQQGEwJOTDEQMA4GA1UECBMHVXRyZWNodDEQMA4GA1UEBxMHVXRyZWNodDEeMBwG\nA1UEChMVTWljaGFlbCB2YW4gVGVsbGluZ2VuMRwwGgYDVQQDExN3d3cucHl0aG9u\nLXplZXAub3JnMSMwIQYJKoZIhvcNAQkBFhRpbmZvQHB5dGhvbi16ZWVwLm9yZzAe\nFw0xNzAxMjUxOTI3NTJaFw0yNzAxMjMxOTI3NTJaMIGUMQswCQYDVQQGEwJOTDEQ\nMA4GA1UECBMHVXRyZWNodDEQMA4GA1UEBxMHVXRyZWNodDEeMBwGA1UEChMVTWlj\naGFlbCB2YW4gVGVsbGluZ2VuMRwwGgYDVQQDExN3d3cucHl0aG9uLXplZXAub3Jn\nMSMwIQYJKoZIhvcNAQkBFhRpbmZvQHB5dGhvbi16ZWVwLm9yZzCCASIwDQYJKoZI\nhvcNAQEBBQADggEPADCCAQoCggEBAMq1sZUbZwE+6tiIFhGkFAsBvtIDbqkzT1It\n3y2f+1yO5TgXpk092HgmXO320y6wAR/JeDRHVxufAhWvzJHbJtOV7eBt2r62E/gj\nWQN7Tn+Nk7BiAef1b6nfS0uLoQVKNqqnE1M9VQPIz+wimNuXavESxHdYMN5S4zxq\nmGuvbFJBGMwAQriXz/cVBMki3nJcVsfpMtj6fAAFz6Q7ZRnW/a7M8WIUibXHvyhL\nG2amgkPWtmQCXhWriYlLzgzzYoLPL1ECxjWB3JhJuEr1ZEkoL6SnpAJNYAudTqi2\nMqafHPdep9QxtjwuW/ZE4+plF5AaGvY41iUGJBPMxucG2jO8QBsCAwEAAaOB/DCB\n+TAdBgNVHQ4EFgQUxd12m9nIS0QO4uIPRy7oerPyVygwgckGA1UdIwSBwTCBvoAU\nxd12m9nIS0QO4uIPRy7oerPyVyihgZqkgZcwgZQxCzAJBgNVBAYTAk5MMRAwDgYD\nVQQIEwdVdHJlY2h0MRAwDgYDVQQHEwdVdHJlY2h0MR4wHAYDVQQKExVNaWNoYWVs\nIHZhbiBUZWxsaW5nZW4xHDAaBgNVBAMTE3d3dy5weXRob24temVlcC5vcmcxIzAh\nBgkqhkiG9w0BCQEWFGluZm9AcHl0aG9uLXplZXAub3JnggkAs4IF6fRL5bkwDAYD\nVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAHTUp/i9FYbvl86By7EvMlZeK\nv6I38IYcrIGzDdbrk8KkilYv7p2Ll8gUJYRFj96iX6Uvn0ACTutFJW9xE2ytBMOu\nUurTBpcpk8k368gfO/fGVi6HzjyFqTnhLkmd3CADIzPN/yg5j2q+mgA3ys6wISBR\naDJR2jGt9sTAkAwkVJdDCFkCwyRfB28mBRnI5SLeR5vQyLT97THPma39xR3FaqYv\nh2q3coXBnaOOcuigiKyIynhJtXH42XlN3TM23b9NK2Oep2e51pxst3uohlDGmB/W\nuzx/hG+kNxy9D+Ms7qNL9+i4nHFOoR034RB/NGTChzTxq2JcXIKPWIo2tslNsg==\n-----END CERTIFICATE-----",
            "wd_client_private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEAyrWxlRtnAT7q2IgWEaQUCwG+0gNuqTNPUi3fLZ/7XI7lOBem\nTT3YeCZc7fbTLrABH8l4NEdXG58CFa/Mkdsm05Xt4G3avrYT+CNZA3tOf42TsGIB\n5/Vvqd9LS4uhBUo2qqcTUz1VA8jP7CKY25dq8RLEd1gw3lLjPGqYa69sUkEYzABC\nuJfP9xUEySLeclxWx+ky2Pp8AAXPpDtlGdb9rszxYhSJtce/KEsbZqaCQ9a2ZAJe\nFauJiUvODPNigs8vUQLGNYHcmEm4SvVkSSgvpKekAk1gC51OqLYypp8c916n1DG2\nPC5b9kTj6mUXkBoa9jjWJQYkE8zG5wbaM7xAGwIDAQABAoIBAQCPlGqYRrSK+Vm2\nzY10KVfZA/95Gd1EE4fXmY4+7tZIFR7ewEGW7HtrfyDLnMywgRIKxdVSkkVs1w/O\n9JpdpXC25bd8A9OwyZ8TX1YpVSmgx1MY2BKpjfrtw6+9bsU6zfoynezeRM72w0Ii\n686Bm5qv7q8iKWFT2DoEDSyw+awsBZQokVTCwHFWdbXZ50mAXoXxovn19DTRNqzD\nyqO8dae9gjk16vap7gRpB60Y/YZ4Rf46X47SlRqTcqgEB/C/1jyGtl3jQlaLq4KL\nPOe1jFZYGUZTctmRvsol4VdSzfITqr/kd3DhJw0LxvXnT6c02wxzKLCSo2HnN6HT\nA7l6eEWhAoGBAPZ46R8qPln9uGhbdYjtEjR+vxDhQcuCNkjn40psEOyXu62cFhFO\nFSj3lVCyRCHIhrUWUzJIQTIPDnczH7iwrWZlqUujjYKs3DJcpu7J5B4ODatklXO+\n2NZa45XEto6ygOPUp7OYZhLlGpjWnC2yp0XLqAEC0URkc1zOTTfJ0VFNAoGBANKL\ntXPJLOZ2F1e3IPkX6y1hfbfbRlyuA2vai/2cAhbld4oZIpm7Yy6Jw4BFuDaUs02P\nnDGBBh6EVgbZNZphZEUhgvglSdJaa2/3cS+1pGcnjmYMj4xywHpOxiomgZ8Xa1LW\nZuJdD2SajS0yPYcrEDg+xBQBvDpE0NEIka6Zu6MHAoGBAMVbKegPjl/GvuupGGMs\n2Z/5QYsFpAaN3GPicmh8Qc0A7oHkcvMmX+Eu5nv4Un/urpbAKpwfqTypO78My8C6\nkA5nJvlvG/ff7G3TLMQWGzhJrn5oCxfkYIK7wnKBUmDO5FAKTsKLLGjC1No/Nk2N\nOU209nDgzaqC+LD+bGxYiOgdAoGAWFtXD7s6Q5EFZMMubDqUcFv8dV7pHVXNi8KQ\ngyKoYdF0pBi+Q4O3ML2RtNANaaJnyMHey4uY9M+WhpM7AomimbxhiR+k5kkZ00gl\nUN9Kmhuoj7zvtQInMmzCjsfQF+KtIHtne9GP9ylA29m8pm/1A5WblcXQpydf9olB\nEePkMZsCgYABr07cGT31CXxrbQwDTgiQJm2JHq/wIR+q0eys1aiMvKRN+0arfqvz\nj8zPK6B9SRcCXY4XAda3rilsF/7eHf2zkg/0kHV6NqaSWFEA8yqAoIqpc03cE/ef\nlUgGakZ6Wb0sucIRB40loAZIu0lN0kF45K1P8JDHg74jk6uM2xnZvg==\n-----END RSA PRIVATE KEY-----",
            "wd_ssl_cert": "stack/process_deltas/workday.pem",
            "wd_tenant_url": "stack/process_deltas/tests/zeep/",
            "wd_api_version": "v1.0",
            "wd_username": "wd_username",
            "wd_tenant": "wd_tenant",
        }
        session = boto3.session.Session()
        conn = session.client("secretsmanager", region_name="us-east-1")
        response = conn.create_secret(Name="mysupersecret", SecretString=json.dumps(secrets))
        os.environ["SECRETS"] = "mysupersecret"
        yield conn


"""S3"""


@pytest.fixture(scope="function")
def s3_data_bucket(aws_credentials):
    """Creates an s3 bucket and a new_run/ and prev_run/ prefix for each hierarchy and stores in env vars"""
    with mock_s3():
        main_bucket = os.environ["BUCKET"] = "main_bucket"
        backup_bucket = os.environ["BACKUP_BUCKET"] = "backup_bucket"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=main_bucket)
        conn.create_bucket(Bucket=backup_bucket)

        # create new_run and prev_run prefix for each data source
        data = json.dumps({"test": "some", "data": "from", "rds": "and", "g11": "sources"})
        for hierarchy in HIERARCHIES:
            hierarchy_upper = hierarchy.upper()
            new_pref = os.environ[f"{hierarchy_upper}_NEW"] = f"{hierarchy}_new_run/"
            old_pref = os.environ[f"{hierarchy_upper}_OLD"] = f"{hierarchy}_prev_run/"

            conn.put_object(Bucket=main_bucket, Key=f"{new_pref}{hierarchy}.json", Body=data)
            conn.put_object(Bucket=main_bucket, Key=f"{old_pref}{hierarchy}.json", Body=data)

        # also need files for cost center reduced
        reduced_pref = os.environ["COSTCENTER_REDUCED"] = "costcenter_reduced_run/"
        data = json.dumps({"source": reduced_pref, "test": "data", "from": "rds", "and": "g11"})
        conn.put_object(Bucket=main_bucket, Key=f"{reduced_pref}costcenter.json", Body=data)

        yield conn


@pytest.fixture(scope="function")
def s3_error_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3")
        conn.create_bucket(Bucket="error_logging_bucket")
        os.environ["ERROR_BUCKET"] = "error_logging_bucket"
        yield conn


"""SQS"""


@pytest.fixture(scope="function")
def sqs_fifo(aws_credentials):
    """Creates a FIFO SQS queue, adds to each of the hierarchies environ vars"""
    with mock_sqs():
        conn = boto3.client("sqs", region_name="us-east-1")
        resp = conn.create_queue(
            QueueName="test-queue.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
        )
        queue_url = resp["QueueUrl"]
        response = conn.get_queue_attributes(QueueUrl=queue_url)
        response["Attributes"].should.contain("FifoQueue")
        response["Attributes"]["FifoQueue"].should.equal("true")

        # set the environment vars for each
        for hierarchy in HIERARCHIES:
            os.environ[f"{hierarchy.upper()}_SQS_URL"] = queue_url

        yield conn


@pytest.fixture(scope="function")
def sqs(aws_credentials):
    """Creates a regular SQS queue, this is only used for costcenter"""
    with mock_sqs():
        conn = boto3.client("sqs", region_name="us-east-1")
        resp = conn.create_queue(
            QueueName="test-queue",
            Attributes={"FifoQueue": "false", "ContentBasedDeduplication": "false"},
        )
        queue_url = resp["QueueUrl"]
        response = conn.get_queue_attributes(QueueUrl=queue_url)

        # set the environment vars for each
        os.environ["COSTCENTER_SQS_URL"] = queue_url

        yield conn


"""STEP FUNCTION"""

simple_definition = (
    '{"Comment": "An example of the Amazon States Language using a choice state.",'
    '"StartAt": "DefaultState",'
    '"States": '
    '{"DefaultState": {"Type": "Fail","Error": "DefaultStateError","Cause": "No Matches!"}}}'
)

account_id = None


@mock_sts
def _get_account_id():
    global account_id
    if account_id:
        return account_id
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    return account_id


def _get_default_role():
    return "arn:aws:iam::" + _get_account_id() + ":role/unknown_sf_role"


@mock_sts
@pytest.fixture(scope="function")
def step_function(aws_credentials):
    """Creates a test step function that can be executed, returns Arn to environ vars"""
    with mock_stepfunctions():
        conn = boto3.client("stepfunctions")
        response = conn.create_state_machine(
            name="example_step_function",
            definition=str(simple_definition),
            roleArn=_get_default_role(),
        )
        os.environ["STEP_FUNC_ARN"] = response["stateMachineArn"]
        yield conn


"""LAMBDA"""


def _process_lambda(func_str):
    zip_output = io.BytesIO()
    zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
    zip_file.writestr("lambda_function.py", func_str)
    zip_file.close()
    zip_output.seek(0)
    return zip_output.read()


def get_test_zip_file():
    pfunc = """
import time
    
def lambda_handler(event, context):
    print("custom log event")
    time.sleep(5)
    return event
"""
    return _process_lambda(pfunc)


def get_role_name():
    with mock_iam():
        iam = boto3.client("iam")
        try:
            return iam.get_role(RoleName="my-role")["Role"]["Arn"]
        except ClientError:
            return iam.create_role(
                RoleName="my-role", AssumeRolePolicyDocument="some policy", Path="/my-path/",
            )["Role"]["Arn"]


@pytest.fixture(scope="function")
def lambdas(aws_credentials):
    """Creates two Lambda functions that can be executed, adds their Arn to environ vars"""
    with mock_lambda():
        conn = boto3.client("lambda")
        response1 = conn.create_function(
            FunctionName="testFunction921",
            Runtime="python3.7",
            Role=get_role_name(),
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": get_test_zip_file()},
            Description="test lambda function",
            Timeout=30,
            MemorySize=128,
            Publish=True,
        )
        response2 = conn.create_function(
            FunctionName="testFunction926",
            Runtime="python3.7",
            Role=get_role_name(),
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": get_test_zip_file()},
            Description="test lambda function",
            Timeout=30,
            MemorySize=128,
            Publish=True,
        )
        os.environ["GET_NINETWOONE_ARN"] = response1["FunctionArn"]
        os.environ["GET_NINETWOSIX_ARN"] = response2["FunctionArn"]

        yield conn


"""ATHENA"""


@pytest.fixture(scope="function")
def athena(aws_credentials):
    """Moto's Athena is still not developed.. not much to test"""

    database = "athena_db"
    workgroup = "athena_workgroup"
    s3_output = "s3://bucket-name/prefix/"
    results = {"OutputLocation": s3_output}

    with mock_athena():
        conn = boto3.client("athena")

        # create workgroup
        conn.create_work_group(
            Name=workgroup,
            Description="Test work group",
            Configuration={"ResultConfiguration": results},
        )

        # create database
        conn.start_query_execution(
            QueryString=f"create database {database}", ResultConfiguration=results
        )

        # add named queries (these are created in terraform)
        for hierarchy in HIERARCHIES:
            response = conn.create_named_query(
                Name=f"{hierarchy}-get-deltas",
                Database=database,
                QueryString="SELECT * FROM table1",
            )
            os.environ[f"{hierarchy.upper()}_QUERY"] = response["NamedQueryId"]

        # costcenter reduction
        response = conn.create_named_query(
            Name=f"costcenter-reduced-query",
            Database=database,
            QueryString="SELECT * FROM table1",
        )
        os.environ[f"COSTCENTER_REDUCED_QUERY"] = response["NamedQueryId"]

        # set athena environment variables
        os.environ["ATHENA_DB"] = database
        os.environ["ATHENA_WG"] = workgroup
        os.environ["ATHENA_OUTPUT"] = s3_output

        yield conn

