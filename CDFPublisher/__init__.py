import base64
import json
import logging
import os
import pickle

from datetime import datetime

import azure.functions as func
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries, ExtractionPipelineRun, Event, Relationship

ts_cache = set()
cognite_client = None
EXTRACTOR_PIPELINE_XID = os.getenv("EXTRACTOR_PIPELINE_EXTERNAL_ID", "function_app")
DATASET_EXTERNAL_ID = os.getenv("DATASET_EXTERNAL_ID", None)
DATASET_ID = None
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

    client = CogniteClient(
        token_url=TOKEN_URL,
        token_client_id=CLIENT_ID,
        token_client_secret=CLIENT_SECRET,
        token_scopes=SCOPES,
        project=COGNITE_PROJECT,
        base_url=BASE_URL,
        client_name="azure_function_publisher",
    )

    client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="success", external_id=EXTRACTOR_PIPELINE_XID))

    if DATASET_EXTERNAL_ID:
        dataset = client.data_sets.retrieve(external_id=DATASET_EXTERNAL_ID)
        if dataset:
            DATASET_ID = dataset.id

    return client

def add_dp(msg, datapoints):
    #{'NodeId': 'http://www.prosysopc.com/OPCUA/SimulationNodes/#i=1006', 'ApplicationUri': 'urn:demo-opcua.mqizqljncwouhpx5ypmrxna13b.fx.internal.cloudapp.net:OPCUA:SimulationServer', 'Value': {'Value': -1.333333, 'SourceTimestamp': '2022-03-15T22:10:20Z'}}
    xid = msg['NodeId']
    #2022-03-23T11:54:07.1Z
    #2022-03-23T11:54:07.1Z
    if len(msg['Value']['SourceTimestamp']) > 21:
        dt = int(datetime.strptime(msg['Value']['SourceTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
    else:
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
    global cognite_client
    
    if not cognite_client:
        cognite_client = create_cognite_client()

    base64_bytes = event.get_json()['body'].encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    msg = json.loads(message_bytes.decode('ascii'))

    logging.info("MESSAGE")
    logging.info(msg)

    datapoints = {}
    #{'start_time': '2022-08-29 10:47:47', 'type': 'Anomaly', 'subtype': 'Cork', 'description': 'Missing cork', 'asset_ids': [7396902715379060], 'id': 4468161180921186, 'last_updated_time': '2022-08-29 10:47:47', 'created_time': '2022-08-29 10:47:47'}
    
    ## event
    if 'type' in msg and msg['type'] == 'Anomaly':

        cur_ev = cognite_client.events.retrieve(external_id=msg['external_id'])

        ev = Event(external_id=msg['external_id'], start_time=int(datetime.strptime(f"{msg['start_time']}Z", "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000), type=msg['type'], subtype=msg['subtype'], description=msg['description'], asset_ids=msg['asset_ids'])
        
        if 'end_time' in msg:
            ev.end_time = int(datetime.strptime(f"{msg['end_time']}Z", "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000)

        if cur_ev:
            cognite_client.events.update(ev)
        else:
            cognite_client.events.create(ev)

    if 'image' in msg:
        logging.info("Image received")
        image_bytes = msg['image'].encode('utf-8')
        image = base64.b64decode(image_bytes)

        cognite_client.files.upload_bytes(image, name=msg['name'], asset_ids=msg['asset_ids'], external_id=msg['name'], mime_type="image/jpeg")
        rel = Relationship(external_id=f"event_{msg['name']}", target_external_id=msg['name'], target_type="File", source_external_id=msg['event'], source_type="Event", confidence=1)
        cognite_client.relationships.create(rel)


    ## datapoints
    if 'datapoints' in msg:
        logging.info("Insert datapoints")
        datapoints = pickle.loads(base64.b64decode(msg['datapoints'].encode('utf-8')))
        cognite_client.datapoints.insert_multiple(datapoints)

    try:
        cognite_client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="seen", external_id=EXTRACTOR_PIPELINE_XID))
    except:
        cognite_client = create_cognite_client()
        cognite_client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="seen", external_id=EXTRACTOR_PIPELINE_XID))
