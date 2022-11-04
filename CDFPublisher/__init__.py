import base64
import json
import logging
import os
import pickle

import azure.functions as func
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (Event, ExtractionPipelineRun,
                                         Relationship)

from CDFPublisher import data_point_insertion_request_pb2

ts_cache = set()
cognite_client = None
EXTRACTOR_PIPELINE_XID = os.getenv("EXTRACTOR_PIPELINE_EXTERNAL_ID", "function_app")
DATASET_EXTERNAL_ID = os.getenv("DATASET_EXTERNAL_ID", None)
DATASET_ID = None
LABEL_MAPPING = { 0: "Cork", 1: "Missing cork" }

# Contact Project Administrator to get these
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
BASE_URL = os.getenv("BASE_URL")
COGNITE_PROJECT = os.getenv("COGNITE_PROJECT")
SCOPES = [f"{BASE_URL}/.default"]
CLIENT_SECRET = os.getenv("CLIENT_SECRET")  # store secret in env variable

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

def create_cognite_client():
    global DATASET_ID
    logging.debug(f"token_url={TOKEN_URL}, token_client_id={CLIENT_ID}, token_client_secret=(hidden), token_scopes={SCOPES}, project={COGNITE_PROJECT}, base_url={BASE_URL}")

    creds = OAuthClientCredentials(token_url=TOKEN_URL, client_id= CLIENT_ID, scopes= SCOPES, client_secret= CLIENT_SECRET)
    cnf = ClientConfig(client_name="azure_function_publisher", project=COGNITE_PROJECT, credentials=creds, base_url=BASE_URL)
    client = CogniteClient(cnf)

    client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="success", external_id=EXTRACTOR_PIPELINE_XID))

    if DATASET_EXTERNAL_ID:
        dataset = client.data_sets.retrieve(external_id=DATASET_EXTERNAL_ID)
        if dataset:
            DATASET_ID = dataset.id

    return client

def main(event: func.EventGridEvent):
    global ts_cache
    global cognite_client
    
    logging.info(event.get_json())

    if not cognite_client:
        cognite_client = create_cognite_client()

    base64_bytes = event.get_json()['body'].encode('utf-8')

    message_bytes = base64.b64decode(base64_bytes)

    try:
        dps = data_point_insertion_request_pb2.DataPointInsertionRequest()
        dps.ParseFromString(message_bytes)

        datapoints={}

        for item in dps.items:
            if len(item.numericDatapoints.datapoints) > 0:
                datapoints.append({"externalId": item.externalId, "datapoints": item.numericDatapoints.datapoints})
            elif len(item.stringDatapoints.datapoints):
                datapoints.append({"externalId": item.externalId, "datapoints": item.stringDatapoints.datapoints})
        
        logging.info("Insert datapoints from Protobuf")
        cognite_client.datapoints.insert_multiple(datapoints)

    except:

        msg = json.loads(message_bytes.decode('utf-8'))
        datapoints = {}
        ## event
        if 'type' in msg and msg['type'] == 'Anomaly':
            logging.info(msg)
            cur_ev = cognite_client.events.retrieve(external_id=msg['external_id'])

            logging.info(msg)
            asset_ids = []
            for xid in msg['asset_external_ids']:
                asset_ids.append(cognite_client.assets.retrieve(external_id=xid).id)

            ev = Event(external_id=msg['external_id'], start_time=int(msg['start_time']), type=msg['type'], subtype=msg['subtype'], description=msg['description'], asset_ids=asset_ids)
            
            if 'end_time' in msg:
                ev.end_time = int(msg['end_time'])

            if cur_ev:
                cognite_client.events.update(ev)
            else:
                cognite_client.events.create(ev)

        if 'image' in msg:
            logging.info(f"Image received {msg['name']}")
            #logging.info(msg)

            image_bytes = msg['image'].encode('utf-8')
            image = base64.b64decode(image_bytes)

            asset_ids = []
            for xid in msg['asset_external_ids']:
                asset_ids.append(cognite_client.assets.retrieve(external_id=xid).id)

            cdf_file = cognite_client.files.retrieve(external_id=msg['name'])
            if not cdf_file:
                cdf_file = cognite_client.files.upload_bytes(image, name=msg['name'], asset_ids=asset_ids, external_id=msg['name'], mime_type="image/jpeg", data_set_id=DATASET_ID)
                rel = Relationship(external_id=f"event_{msg['name']}", target_external_id=msg['name'], target_type="File", source_external_id=msg['event'], source_type="Event", confidence=1)
                cognite_client.relationships.create(rel)
            else:
                logging.warning(f"File {msg['name']} already exists")
                


            #annotations
            if "coordinates" in msg:
                for box in msg['coordinates']:
                    logging.info(box)
                    annotation = {
                        "items": [
                            {
                                "annotatedResourceId": cdf_file.id,
                                "annotatedResourceType": "file",
                                "annotationType": "images.ObjectDetection",
                                "creatingApp": "Fusion: Vision",
                                "creatingAppVersion": "0.0.1",
                                "creatingUser": None,
                                "data": {
                                    "boundingBox": {
                                        "xMax": float(box['xMax']),
                                        "xMin": float(box['xMin']),
                                        "yMax": float(box['yMax']),
                                        "yMin": float(box['yMin']),
                                    },
                                    "confidence": float(box['confidence']),
                                    "label": LABEL_MAPPING[int(box['label_id'])],
                                },
                            }
                        ]
                    }
                    cognite_client.post(f"/api/v1/projects/{COGNITE_PROJECT}/annotations/suggest", annotation)




            ## datapoints
            if 'datapoints' in msg:
                logging.debug("Insert datapoints")
                datapoints = pickle.loads(base64.b64decode(msg['datapoints'].encode('utf-8')))
                cognite_client.datapoints.insert_multiple(datapoints)

    try:
        cognite_client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="seen", external_id=EXTRACTOR_PIPELINE_XID))
    except:
        cognite_client = create_cognite_client()
        cognite_client.extraction_pipeline_runs.create(ExtractionPipelineRun(status="seen", external_id=EXTRACTOR_PIPELINE_XID))
