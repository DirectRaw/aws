import os

SAP_URL = "https://sap.host.com/SENDSOAP"


class Response:
    def __init__(self, content, status_code):
        self.status_code = status_code
        self.content = content


def get_local_file(file):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"data/{file}")
    return path


def get_xml(file):
    path = get_local_file(file)
    with open(path, "r") as f:
        return f.read().strip()
