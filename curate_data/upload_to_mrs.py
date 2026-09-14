#!/usr/bin/env python3
import os
import requests
import urllib3
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timedelta
import json as _json
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_next_dataset_number(counter_file: str = ".dataset_counter_local") -> int:
    counter_path = Path(counter_file)
    current = 0
    if counter_path.exists():
        try:
            current = int(counter_path.read_text().strip())
        except:
            pass
    next_num = current + 1
    counter_path.write_text(str(next_num))
    return next_num

class SLICESDatasetUploaderLocal:
    def __init__(self, use_client_credentials=False):
        self.keycloak_url = os.environ["KEYCLOAK_URL"] 
        self.mini_dmi_url = os.environ["MRS_BACKEND_URL"]
        
        self.realm = "slices"
        self.client_id = "mini-dmi"
        self.verify_ssl = False
        self.access_token = None
        self.use_client_credentials = use_client_credentials
    
    def authenticate(self, username: str = None, password: str = None, client_secret: str = None) -> str:
        token_url = os.environ["OIDC_TOKEN_URL"]
        
        if self.use_client_credentials:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": client_secret,
            }
        else:
            data = {
                "grant_type": "password",
                "client_id": self.client_id,
                "username": username,
                "password": password,
            }
            if client_secret:
                data["client_secret"] = client_secret
        
        response = requests.post(token_url, data=data, verify=self.verify_ssl)
        
        if response.status_code != 200:
            print(f"[AUTH] Authentication failed ({response.status_code}): {response.text}")
            response.raise_for_status()
        
        self.access_token = response.json()["access_token"]
        print("[AUTH] Authentication completed successfully")
        return self.access_token
    
    def upload_metadata_and_file(self, metadata: Dict[str, Any], file_path: str) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        file_path = Path(file_path)
        
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{self.mini_dmi_url}/v0.2/digital-objects/upload",
                headers=headers,
                files={"file": (file_path.name, f, "text/csv")},
                data={"metadata": _json.dumps(metadata)},
                verify=self.verify_ssl
            )

        if response.status_code not in (200, 201):
            print(f"\n[ERROR FROM BACKEND] {response.status_code}: {response.text}\n")
            
        response.raise_for_status()
        digital_object_id = response.json()
        print(f"[UPLOAD] Success - Digital Object ID: {digital_object_id}")
        return {"digitalObjectId": digital_object_id}
    
    def upload_complete_workflow(self, username, password, metadata, file_path, client_secret=None):
        self.authenticate(username, password, client_secret)
        return self.upload_metadata_and_file(metadata, file_path)


def main(path_input_dataset_file):
    # ==================== LOCAL CONFIGURATION ====================
    USE_CLIENT_CREDENTIALS = True
    CLIENT_SECRET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"
    
    # =============================================================
    
    file_path = Path(path_input_dataset_file)
    file_size = file_path.stat().st_size if file_path.exists() else 0
    
    now = datetime.now()
    start_time = (now - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    end_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    created_at = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    
    dataset_num = get_next_dataset_number()
    dataset_id = f"local-dataset-{dataset_num:03d}"

    metadata = {
        "$type": "Dataset",
        "identifier": dataset_id,
        "alternateIdentifier": "string",
        "downloadUrl": None,
        "systemCreationDate": None,
        "internalIdentifier": None,
        "paymentModel": "http://something-testtesttest",
        "pricing": None,
        "scientificDomains": ["quia", "necessitatibus", "non", "vero"],
        "scientificSubdomains": ["alias", "ipsa", "quo"],
        "address": "",
        "dateTimeStart": start_time,
        "dateTimeEnd": end_time,
        "locations": [
            {
                "locationName": "Local Test Location",
                "locationAddress": "Local Server Location",
                "postalCode": None,
                "country": "Italy",
                "latitude": 41.9028,
                "longitude": 12.4964,
                "altitude": 35.0
            }
        ],
        "byteSize": file_size,
        "duration": "19:24:52.1674420",
        "format": "application/json",
        "medium": "",
        "compressionFormat": "none",
        "fileMetadata": "Local test dataset",
        "dataStandard": "JSON",
        "encodingFormat": "UTF-8",
        "creators": [
            {
                "firstName": "Local",
                "lastName": "User",
                "email": "local.user@example.com",
                "phone": "+39-123-456-7890",
                "position": "Data Engineer",
                "organization": "SLICES Local",
                "orcId": "http://orcid.org/0000-0000-0000-0001",
                "website": "https://slices-ri.eu"
            }
        ],
        "name": "Local Test Dataset",
        "description": "This is a test dataset uploaded to the local environment.",
        "resourceType": "Dataset",
        "subjects": [],
        "keywords": ["local", "test", "validation"],
        "createdAt": created_at,
        "version": "V1",
        "metadataProfile": "Slices_V0_2",
        "contributors": [],
        "accessType": "Physical",
        "accessMode": "FreeConditionally",
        "requiredObjects": [],
        "relatedObjects": [],
        "primaryLanguages": ["en", "it"],
        "otherLanguages": [],
        "contact": None,
        "publicContact": None,
        "rights": "Local test data",
        "rightsUri": "https://slices-ri.eu",
        "accessRights": "Restricted",
        "license": "MIT",
        "licenseUri": "https://opensource.org/licenses/MIT",
        "copyrightsHolder": "SLICES-RI",
        "confidentialityDeclaration": "",
        "specialPermissions": "",
        "restrictions": "",
        "citationRequirements": "Cite me",
        "conditions": "",
        "disclaimer": "",
        "provenance": "Local test upload",
        "statistics": _json.dumps({
            "records": {
                "count": 100,
                "mean": 50.5,
                "std": 29.01,
                "min": 1.0,
                "max": 100.0
            }
        })
    }
    
    uploader = SLICESDatasetUploaderLocal(use_client_credentials=USE_CLIENT_CREDENTIALS)
    
    result = uploader.upload_complete_workflow(None, None, metadata, path_input_dataset_file, CLIENT_SECRET)
    
    with open("upload_result_local.json", "w") as f:
        _json.dump(result, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/csv/file")
    main(sys.argv[1])
