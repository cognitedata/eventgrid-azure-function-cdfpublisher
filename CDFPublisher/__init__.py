import base64
import json
import logging
import os
from datetime import datetime

import azure.functions as func
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries

ts_cache = set()


def create_cognite_client():

    # Contact Project Administrator to get these
    TENANT_ID = os.getenv("TENANT_ID")
    CLIENT_ID = os.getenv("CLIENT_ID")
    BASE_URL = os.getenv("BASE_URL")
    COGNITE_PROJECT = os.getenv("COGNITE_PROJECT")
    SCOPES = [f"{BASE_URL}/.default"]
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")  # store secret in env variable

    TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    logging.debug(f"token_url={TOKEN_URL}, token_client_id={CLIENT_ID}, token_client_secret=(hidden), token_scopes={SCOPES}, project={COGNITE_PROJECT}, base_url={BASE_URL}")

    return CogniteClient(
        token_url=TOKEN_URL,
        token_client_id=CLIENT_ID,
        token_client_secret=CLIENT_SECRET,
        token_scopes=SCOPES,
        project=COGNITE_PROJECT,
        base_url=BASE_URL,
        client_name="azure_function_publisher",
    ) 

def add_dp(msg, datapoints):
    #{'NodeId': 'http://www.prosysopc.com/OPCUA/SimulationNodes/#i=1006', 'ApplicationUri': 'urn:demo-opcua.mqizqljncwouhpx5ypmrxna13b.fx.internal.cloudapp.net:OPCUA:SimulationServer', 'Value': {'Value': -1.333333, 'SourceTimestamp': '2022-03-15T22:10:20Z'}}
    xid = msg['NodeId']

    dt = int(datetime.strptime(msg['Value']['SourceTimestamp'], "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000)
    if xid in datapoints:
        datapoints[xid].append((dt, msg['Value']['Value']))
    else:
        datapoints[xid] = [(dt, msg['Value']['Value'])]

def get_dp(datapoints):
    dp = []
    for xid in datapoints:
        dp.append({"externalId": xid, "datapoints": datapoints[xid]})
    return dp

def main(event: func.EventGridEvent):
    global ts_cache

    client = create_cognite_client()

    messages = json.loads(base64.b64decode(event.get_json()['body']))
    datapoints = {}
    for msg in messages:
        xid = msg['NodeId']
        if not xid in ts_cache:
            if client.time_series.retrieve(external_id=xid) == None:
                client.time_series.create(TimeSeries(name=xid, external_id=xid, is_string=False))
            ts_cache.add(xid)

        add_dp(msg, datapoints)


    if len(datapoints) > 0:
        client.datapoints.insert_multiple(get_dp(datapoints))
