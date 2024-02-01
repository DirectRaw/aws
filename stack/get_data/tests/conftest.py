import os
import io
import pytest
import moto
from moto import mock_secretsmanager, mock_s3
from mock import patch
import boto3
import json
import sure  # noqa
from botocore.exceptions import ClientError

HIERARCHIES = [
    "onezeroone",
    "ninetwoone",
    "ninetwosix",
    "building",
    "site",
    "costcenter",
    "companycode",
]


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture(request):
    patched = patch.dict(
        os.environ, {"AWS_DEFAULT_REGION": "us-east-1", "TF_WORKSPACE": "test",},
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
            "sap_host": "sap.host.com",
            "sap_url": "/SENDSOAP",
            "sap_target_system": "ABC",
            "sap_application_id": "12345678",
            "sap_username": "username",
            "sap_password": "password",
            "sap_ssl_cert": "stack/get_data/certificates/agwa_server.pem",
            "sap_client_cert": "-----BEGIN CERTIFICATE-----\nMIIEqjCCA5KgAwIBAgIJALOCBen0S+W5MA0GCSqGSIb3DQEBBQUAMIGUMQswCQYD\nVQQGEwJOTDEQMA4GA1UECBMHVXRyZWNodDEQMA4GA1UEBxMHVXRyZWNodDEeMBwG\nA1UEChMVTWljaGFlbCB2YW4gVGVsbGluZ2VuMRwwGgYDVQQDExN3d3cucHl0aG9u\nLXplZXAub3JnMSMwIQYJKoZIhvcNAQkBFhRpbmZvQHB5dGhvbi16ZWVwLm9yZzAe\nFw0xNzAxMjUxOTI3NTJaFw0yNzAxMjMxOTI3NTJaMIGUMQswCQYDVQQGEwJOTDEQ\nMA4GA1UECBMHVXRyZWNodDEQMA4GA1UEBxMHVXRyZWNodDEeMBwGA1UEChMVTWlj\naGFlbCB2YW4gVGVsbGluZ2VuMRwwGgYDVQQDExN3d3cucHl0aG9uLXplZXAub3Jn\nMSMwIQYJKoZIhvcNAQkBFhRpbmZvQHB5dGhvbi16ZWVwLm9yZzCCASIwDQYJKoZI\nhvcNAQEBBQADggEPADCCAQoCggEBAMq1sZUbZwE+6tiIFhGkFAsBvtIDbqkzT1It\n3y2f+1yO5TgXpk092HgmXO320y6wAR/JeDRHVxufAhWvzJHbJtOV7eBt2r62E/gj\nWQN7Tn+Nk7BiAef1b6nfS0uLoQVKNqqnE1M9VQPIz+wimNuXavESxHdYMN5S4zxq\nmGuvbFJBGMwAQriXz/cVBMki3nJcVsfpMtj6fAAFz6Q7ZRnW/a7M8WIUibXHvyhL\nG2amgkPWtmQCXhWriYlLzgzzYoLPL1ECxjWB3JhJuEr1ZEkoL6SnpAJNYAudTqi2\nMqafHPdep9QxtjwuW/ZE4+plF5AaGvY41iUGJBPMxucG2jO8QBsCAwEAAaOB/DCB\n+TAdBgNVHQ4EFgQUxd12m9nIS0QO4uIPRy7oerPyVygwgckGA1UdIwSBwTCBvoAU\nxd12m9nIS0QO4uIPRy7oerPyVyihgZqkgZcwgZQxCzAJBgNVBAYTAk5MMRAwDgYD\nVQQIEwdVdHJlY2h0MRAwDgYDVQQHEwdVdHJlY2h0MR4wHAYDVQQKExVNaWNoYWVs\nIHZhbiBUZWxsaW5nZW4xHDAaBgNVBAMTE3d3dy5weXRob24temVlcC5vcmcxIzAh\nBgkqhkiG9w0BCQEWFGluZm9AcHl0aG9uLXplZXAub3JnggkAs4IF6fRL5bkwDAYD\nVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAHTUp/i9FYbvl86By7EvMlZeK\nv6I38IYcrIGzDdbrk8KkilYv7p2Ll8gUJYRFj96iX6Uvn0ACTutFJW9xE2ytBMOu\nUurTBpcpk8k368gfO/fGVi6HzjyFqTnhLkmd3CADIzPN/yg5j2q+mgA3ys6wISBR\naDJR2jGt9sTAkAwkVJdDCFkCwyRfB28mBRnI5SLeR5vQyLT97THPma39xR3FaqYv\nh2q3coXBnaOOcuigiKyIynhJtXH42XlN3TM23b9NK2Oep2e51pxst3uohlDGmB/W\nuzx/hG+kNxy9D+Ms7qNL9+i4nHFOoR034RB/NGTChzTxq2JcXIKPWIo2tslNsg==\n-----END CERTIFICATE-----",
            "sap_client_private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEAyrWxlRtnAT7q2IgWEaQUCwG+0gNuqTNPUi3fLZ/7XI7lOBem\nTT3YeCZc7fbTLrABH8l4NEdXG58CFa/Mkdsm05Xt4G3avrYT+CNZA3tOf42TsGIB\n5/Vvqd9LS4uhBUo2qqcTUz1VA8jP7CKY25dq8RLEd1gw3lLjPGqYa69sUkEYzABC\nuJfP9xUEySLeclxWx+ky2Pp8AAXPpDtlGdb9rszxYhSJtce/KEsbZqaCQ9a2ZAJe\nFauJiUvODPNigs8vUQLGNYHcmEm4SvVkSSgvpKekAk1gC51OqLYypp8c916n1DG2\nPC5b9kTj6mUXkBoa9jjWJQYkE8zG5wbaM7xAGwIDAQABAoIBAQCPlGqYRrSK+Vm2\nzY10KVfZA/95Gd1EE4fXmY4+7tZIFR7ewEGW7HtrfyDLnMywgRIKxdVSkkVs1w/O\n9JpdpXC25bd8A9OwyZ8TX1YpVSmgx1MY2BKpjfrtw6+9bsU6zfoynezeRM72w0Ii\n686Bm5qv7q8iKWFT2DoEDSyw+awsBZQokVTCwHFWdbXZ50mAXoXxovn19DTRNqzD\nyqO8dae9gjk16vap7gRpB60Y/YZ4Rf46X47SlRqTcqgEB/C/1jyGtl3jQlaLq4KL\nPOe1jFZYGUZTctmRvsol4VdSzfITqr/kd3DhJw0LxvXnT6c02wxzKLCSo2HnN6HT\nA7l6eEWhAoGBAPZ46R8qPln9uGhbdYjtEjR+vxDhQcuCNkjn40psEOyXu62cFhFO\nFSj3lVCyRCHIhrUWUzJIQTIPDnczH7iwrWZlqUujjYKs3DJcpu7J5B4ODatklXO+\n2NZa45XEto6ygOPUp7OYZhLlGpjWnC2yp0XLqAEC0URkc1zOTTfJ0VFNAoGBANKL\ntXPJLOZ2F1e3IPkX6y1hfbfbRlyuA2vai/2cAhbld4oZIpm7Yy6Jw4BFuDaUs02P\nnDGBBh6EVgbZNZphZEUhgvglSdJaa2/3cS+1pGcnjmYMj4xywHpOxiomgZ8Xa1LW\nZuJdD2SajS0yPYcrEDg+xBQBvDpE0NEIka6Zu6MHAoGBAMVbKegPjl/GvuupGGMs\n2Z/5QYsFpAaN3GPicmh8Qc0A7oHkcvMmX+Eu5nv4Un/urpbAKpwfqTypO78My8C6\nkA5nJvlvG/ff7G3TLMQWGzhJrn5oCxfkYIK7wnKBUmDO5FAKTsKLLGjC1No/Nk2N\nOU209nDgzaqC+LD+bGxYiOgdAoGAWFtXD7s6Q5EFZMMubDqUcFv8dV7pHVXNi8KQ\ngyKoYdF0pBi+Q4O3ML2RtNANaaJnyMHey4uY9M+WhpM7AomimbxhiR+k5kkZ00gl\nUN9Kmhuoj7zvtQInMmzCjsfQF+KtIHtne9GP9ylA29m8pm/1A5WblcXQpydf9olB\nEePkMZsCgYABr07cGT31CXxrbQwDTgiQJm2JHq/wIR+q0eys1aiMvKRN+0arfqvz\nj8zPK6B9SRcCXY4XAda3rilsF/7eHf2zkg/0kHV6NqaSWFEA8yqAoIqpc03cE/ef\nlUgGakZ6Wb0sucIRB40loAZIu0lN0kF45K1P8JDHg74jk6uM2xnZvg==\n-----END RSA PRIVATE KEY-----",
        }
        session = boto3.session.Session()
        conn = session.client("secretsmanager", region_name="us-east-1")
        response = conn.create_secret(
            Name="mysupersecret", SecretString=json.dumps(secrets)
        )
        os.environ["SECRETS"] = "mysupersecret"
        yield conn


"""S3"""


@pytest.fixture(scope="function")
def s3_data_bucket(aws_credentials):
    """Creates an s3 bucket and stores new_run/ prefix for each hierarchy in env vars"""
    with mock_s3():
        bucket = os.environ["BUCKET"] = "main_bucket"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)

        for hierarchy in HIERARCHIES:
            hierarchy_upper = hierarchy.upper()
            os.environ[f"{hierarchy_upper}_NEW"] = f"{hierarchy}_new_run/"

        yield conn
