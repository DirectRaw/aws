import json


def hello(event, context):
    body = {
        "message": "I did it successfully!",
        "input": event,
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
