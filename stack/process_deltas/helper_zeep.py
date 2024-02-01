# import zeep stuff
import os
import xmltodict
from lxml import etree
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client as ZeepClient
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
from zeep.cache import SqliteCache
from tempfile import NamedTemporaryFile as NamedTempFile

# make monkey fixes to Zeep
import xmlsec
import zeep.wsse.signature
from lxml.etree import QName
from zeep import ns
from zeep.exceptions import SignatureVerificationFailed
from zeep.utils import detect_soap_env
from zeep.wsse.signature import Signature, _sign_node
from zeep.wsse.utils import ensure_id, get_security_header

# TLS 1.2
import ssl
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from requests.packages.urllib3.util import ssl_

try:
    from shared.helper import get_secrets, logger
except ModuleNotFoundError:
    import sys

    sys.path.append("../")
    from shared.helper import get_secrets, logger


def _signature_prepare(envelope, key, signature_method, digest_method):
    """Modify Zeep source code: change signature algorithm and remove Security Token Reference"""
    soap_env = detect_soap_env(envelope)

    signature = xmlsec.template.create(
        envelope,
        xmlsec.Transform.EXCL_C14N,
        signature_method or xmlsec.Transform.RSA_SHA1,
    )

    key_info = xmlsec.template.ensure_key_info(signature)
    x509_data = xmlsec.template.add_x509_data(key_info)
    # removing issuer serial as workday doesn't require this but doesn't seem to hate it
    xmlsec.template.x509_data_add_issuer_serial(x509_data)
    xmlsec.template.x509_data_add_certificate(x509_data)
    # Sutiod uses subject name from certificate to request but doesn't seem to require it
    # xmlsec.template.x509_data_add_subject_name(x509_data)

    security = get_security_header(envelope)
    security.insert(0, signature)

    # Perform the actual signing.
    ctx = xmlsec.SignatureContext()
    ctx.key = key
    _sign_node(ctx, signature, envelope.find(QName(soap_env, "Body")), digest_method)
    timestamp = security.find(QName(ns.WSU, "Timestamp"))
    if timestamp != None:
        _sign_node(ctx, signature, timestamp)
    ctx.sign(signature)

    # removed the "SecurityTokenReference bracket around the Key Data"
    sec_token_ref = key_info
    return security, sec_token_ref, x509_data


def _sign_node(ctx, signature, target, digest_method=None):
    """Modify Zeep source code: remove signature Reference URI"""
    node_id = ensure_id(target)
    ctx.register_id(target, "Id", ns.WSU)
    #  removed reference uri
    ref = xmlsec.template.add_reference(signature, xmlsec.Transform.SHA256, uri="")

    # change the Transform method to ENVELOPED, which is the only one workday accepts
    xmlsec.template.add_transform(ref, xmlsec.Transform.ENVELOPED)


# load 2 edited methods
zeep.wsse.signature._sign_node = _sign_node
zeep.wsse.signature._signature_prepare = _signature_prepare


class TlsAdapter(HTTPAdapter):
    """Force TLS 1.2 https://stackoverflow.com/questions/44404084/python-requests-library-using-tlsv1-or-tlsv1-1-despite-upgrading-to-python-2-7 """

    def __init__(self, ssl_options=0, **kwargs):
        self.ssl_options = ssl_options
        super(TlsAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, *pool_args, **pool_kwargs):
        ctx = ssl_.create_urllib3_context(ssl.PROTOCOL_TLSv1_2)
        # extend the default context options, which is to disable ssl2, ssl3
        # and ssl compression, see:
        # https://github.com/shazow/urllib3/blob/6a6cfe9/urllib3/util/ssl_.py#L241
        ctx.options |= self.ssl_options
        self.poolmanager = PoolManager(*pool_args, ssl_context=ctx, **pool_kwargs)


def get_client(local=False, service="Human_Resources"):
    """Return a Zeep Client to make requests to Workday HCM Web Service"""

    # get secrets
    logger.info("Creating Zeep Client")
    secrets = get_secrets(False)
    wd_api_version = secrets["wd_api_version"]
    wd_tenant_base_url = secrets["wd_tenant_url"]
    wd_tenant = secrets["wd_tenant"]

    # username = <USER>@<TENANT>
    wd_username = f"{secrets['wd_username']}@{wd_tenant}"

    # url = <https://wd5-impl-services1.workday.com/ccx/service>/<TENANT>/<SERVICE>
    # service can be "Human_Resources" or "Financial_Management", any others need adding
    # tenant has been tested with PG4 and PG5
    wd_tenant_url = f"{wd_tenant_base_url}/{wd_tenant}/{service}"

    # wsdl = <TENANT_URL w SERVICE>/<API_VERSION>?wsdl
    wd_wsdl_url = f"{wd_tenant_url}/{wd_api_version}?wsdl"

    if local == "Testing":
        # for unit testing, get a local wsdl file
        wd_wsdl_url = wd_tenant_base_url + service + ".wsdl"

    # certificates
    wd_ssl_cert = secrets["wd_ssl_cert"]
    wd_client_cert = secrets["wd_client_cert"]
    wd_client_key = secrets["wd_client_private_key"]

    # server certificate is stored locally
    path = "" if local else "process_deltas/"
    session = Session()
    session.verify = path + wd_ssl_cert

    # force TLS 1.2
    session.mount(
        "https://",
        TlsAdapter(
            ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1 | ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        ),
    )

    # using cache to save wsdl
    cache = SqliteCache(path="/tmp/sqlite.db", timeout=60)

    # set up temporary file paths for requests to read in certificates
    client_cert = NamedTempFile(delete=False, prefix="certificates", suffix=".pem")
    client_cert.write(wd_client_cert.encode())
    client_cert.close()
    private_key = NamedTempFile(delete=False, prefix="certificates", suffix=".pem")
    private_key.write(wd_client_key.encode())
    private_key.close()

    # generate username token and signature based on public/private key
    try:
        #     zeep_client = ZeepClient(
        #         wsdl=wd_wsdl_url,
        #         transport=Transport(session=session, cache=cache),
        #         wsse=UsernameToken(
        #             f"{secrets['test_username']}@{wd_tenant}", secrets["test_password"]
        #         ),
        #     )

        username_token = UsernameToken(wd_username)
        signature = Signature(
            key_file=private_key.name,
            certfile=client_cert.name,
            signature_method=xmlsec.Transform.RSA_SHA1,
            digest_method=xmlsec.Transform.SHA256,
        )

        # set up client
        zeep_client = ZeepClient(
            wsdl=wd_wsdl_url,
            transport=Transport(session=session, cache=cache),
            wsse=[username_token, signature],
        )

        service = zeep_client.create_service(
            f"{{urn:com.workday/bsvc/{service}}}{service}Binding", f"{wd_tenant_url}",
        )
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        # the temp files need to be explicitly deleted, this is security flaw
        os.unlink(client_cert.name)
        os.unlink(private_key.name)

    return zeep_client, service, wd_api_version


def send_request(request, zeep_client, service, version, payload):
    """Sends a web service to Workday using the Zeep client"""

    if request == "Add_Update_Organization":
        with zeep_client.settings(raw_response=True):
            response = zeep_client.service.Add_Update_Organization(
                Organization_Data=payload, version=version
            )
            if response.status_code == 200:
                # there's nothing useful in an Add_Update_Org success response
                logger.info(
                    f"Updated/added organization: {payload['Organization_Reference_ID']}"
                )
                return True
            else:
                # get the error message
                error_msg = get_error_response(response.content)
                logger.error(
                    f"Error sending Add_Update_Organization request: {error_msg}"
                )

                return error_msg

    elif request == "Put_Location":
        with zeep_client.settings(raw_response=True):
            response = zeep_client.service.Put_Location(
                Location_Data=payload, version=version, Add_Only=False
            )
            if response.status_code == 200:
                msg = xmltodict.parse(response.content)
                logger.info(
                    f"Updated/added location: {msg['env:Envelope']['env:Body']['wd:Put_Location_Response']['wd:Location_Reference']['wd:ID'][1]['#text']}"
                )
                return True
            else:
                # get the error message
                error_msg = get_error_response(response.content)
                logger.error(f"Error sending Put_Location request: {error_msg}")

                return error_msg

    elif "Get_Locations" in request:
        with zeep_client.settings(raw_response=True):
            response = zeep_client.service.Get_Locations(
                Request_References=payload["Request_References"],
                Response_Group=payload["Response_Group"],
                version=version,
            )
            if response.status_code == 200:
                # if the site already exists in the tenant, check if active. if it doesn't exist, it will throw 500 error
                inactive = get_inactive_location(response.content)
                if inactive and "Site" in request:
                    # skip site because it's inactive
                    return None
                elif not inactive and "Site" in request:
                    # if it's active site, we want to get its Time Profile
                    return get_time_profile(response.content)
                elif "Building" in request:
                    # for building, just retrieve whether it's inactive or not
                    return inactive
            else:
                # response could potentially be error like, tenant down, etc so if it's anything other than invalid error, throw exception
                error_msg = get_error_response(response.content)
                if "Invalid ID value" not in error_msg:
                    raise Exception(f"Unknown error from Get_Locations: {error_msg}")

                # if it is invalid ID error, it just means the location does not exist in WD yet
                logger.warning(
                    f"Site ID: {payload['DNU_id']} does not exist, adding it..."
                )
                if "Site" in request:
                    # if the site doesn't exist, we will proceed with adding it with default time
                    # don't need to specify if active or not because Put_Locations call defaults to Active
                    return "Standard_Hours_40"
                if "Building" in request:
                    # if building doesn't exist in WD, we will create a building with Inactive=false
                    return False

    elif request == "Put_Cost_Center":
        with zeep_client.settings(raw_response=True):
            response = zeep_client.service.Put_Cost_Center(
                Cost_Center_Data=payload, version=version, Add_Only=False
            )
            if response.status_code == 200:
                msg = xmltodict.parse(response.content)
                logger.info(
                    f"Updated/added costcenter: {msg['env:Envelope']['env:Body']['wd:Put_Cost_Center_Response']['wd:Cost_Center_Reference']['wd:ID'][2]['#text']}"
                )
                return True
            else:
                # get the error message
                error_msg = get_error_response(response.content)
                logger.error(f"Error sending Put_Cost_Center request: {error_msg}")

                return error_msg

    elif request == "Get_Cost_Centers":
        with zeep_client.settings(raw_response=True):
            response = zeep_client.service.Get_Cost_Centers(
                Request_References=payload["Request_References"],
                Response_Group=payload["Response_Group"],
                version=version,
            )
            if response.status_code == 200:
                # if the cost center already exists in the tenant, check if active. if it doesn't exist we will get 500 error
                return get_active_cost_center(response.content)
            else:
                # response could potentially be error like, tenant down, etc so if it's anything other than
                # site doesn't exist error, we throw runtime error
                error_msg = get_error_response(response.content)
                if "Invalid ID value" not in error_msg:
                    raise Exception(f"Unknown error from Get_Cost_Centers: {error_msg}")

                # if this cost center doesn't exist, we will create it and set active=True
                logger.warning(
                    f'Cost Center: {payload["Request_References"]["Cost_Center_Reference"]["ID"]["_value_1"]} does not exist, adding it...'
                )
                return "true"


def get_error_response(content):
    """Parse the error message of a webservice call"""
    msg = xmltodict.parse(content)
    return msg["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["SOAP-ENV:Fault"]["faultstring"]


def get_inactive_location(content):
    """Parse the response for get Location and return true if inactive, false if active"""
    msg = xmltodict.parse(content)

    # parse the resposne for the data that we need
    try:
        location = msg["env:Envelope"]["env:Body"]["wd:Get_Locations_Response"][
            "wd:Response_Data"
        ]["wd:Location"]["wd:Location_Data"]
        loc_id = location["wd:Location_ID"]
        inactive = location["wd:Inactive"]
    except KeyError:
        raise Exception(f"Error getting Active Status from response for this site")

    # if the site is inactive, then we return None and will not send anything to WD
    # inactive can either be 0 or 1
    logger.info(
        f"Retrieved the Inactive Status for location {loc_id} from Workday: {inactive}"
    )
    if inactive == "0":
        return False  # active
    else:
        return True  # inactive


def get_time_profile(content):
    """Get the time profile for the site, if there isn't one and site exists, set to default"""
    msg = xmltodict.parse(content)

    location = msg["env:Envelope"]["env:Body"]["wd:Get_Locations_Response"][
        "wd:Response_Data"
    ]["wd:Location"]["wd:Location_Data"]

    try:
        time_profile = location["wd:Time_Profile_Reference"]["wd:ID"][1]["#text"]
    except KeyError:
        logger.warn(
            f'Site {location["wd:Location_ID"]} does not have a time profile, setting to default'
        )
        time_profile = "Standard_Hours_40"

    return time_profile


def get_active_cost_center(content):
    """Parse the response for Get_Cost_Center for Organization_Active status"""
    msg = xmltodict.parse(content)
    active = msg["env:Envelope"]["env:Body"]["wd:Get_Cost_Centers_Response"][
        "wd:Response_Data"
    ]["wd:Cost_Center"]["wd:Cost_Center_Data"]["wd:Organization_Data"][
        "wd:Organization_Active"
    ]
    return "true" if active == "1" else "false"


# def write_soap(node):
#     """Writes the SOAP call created by Zeep but also removes signature and auth details"""
#     tree_nosig = (
#         '<soap-env:Envelope xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">'
#         + etree.tostring(node).decode().split("</soap-env:Header>")[1]
#     )
#     tree = etree.ElementTree(etree.fromstring(tree_nosig))
#     tree_string = etree.tostring(tree, pretty_print=True)
#     return tree_string

# write to file
# tree.write("test.xml", pretty_print=True)

