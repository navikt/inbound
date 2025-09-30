import base64
import csv
import io
import json
import time
from typing import Any, Generator, Optional

import requests

from ..core.models import Description
from ..sdk.tap import Tap


class AnaplanAuthException(Exception):
    pass


class AnaplanIntegrationService:
    def __init__(self, workspaceID, modelID, exportID, fileID, username, password):
        self.workspaceID = workspaceID
        self.modelID = modelID
        self.exportID = exportID
        self.fileID = fileID
        self.username = username
        self.password = password
        self.base_url = (
            f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}"
        )
        self.auth_url = "https://auth.anaplan.com/token/authenticate"

    def export_information(self) -> dict:
        url = f"{self.base_url}/exports/{self.exportID}"
        respons = requests.get(
            url, headers=self._headers(), data=json.dumps({"localeName": "en_US"})
        )
        return respons.json()

    def trigger_export_task(self) -> dict:
        url = f"{self.base_url}/exports/{self.exportID}/tasks"
        return requests.post(
            url, headers=self._headers(), data=json.dumps({"localeName": "en_US"})
        ).json()

    def export_task_status(self, taskID) -> dict:
        status_url = f"{self.base_url}/exports/{self.exportID}/tasks/{taskID}"
        return requests.get(
            status_url,
            headers=self._headers(),
            data=json.dumps({"localeName": "en_US"}),
        ).json()

    def number_of_file_chunks(self) -> dict:
        url = f"{self.base_url}/files/{self.fileID}/chunks/"
        return requests.get(
            url, headers=self._headers(), data=json.dumps({"localeName": "en_US"})
        ).json()

    def file_chunk(self, chunkID) -> bytes:
        url = f"{self.base_url}/files/{self.fileID}/chunks/{chunkID}"
        respons = requests.get(
            url, headers=self._headers(), data=json.dumps({"localeName": "en_US"})
        )
        return respons.content

    def _auth_response(self) -> requests.Response:
        user = "Basic " + str(
            base64.b64encode(
                (f"{self.username}:{self.password}").encode("utf-8")
            ).decode("utf-8")
        )
        auth_header = {"Authorization": user, "Content-Type": "application/json"}

        return requests.post(
            url=self.auth_url,
            headers=auth_header,
            data=json.dumps({"localeName": "en_US"}),
        )

    def _headers(self, auth_response: Optional[requests.Response] = None):
        auth_response = auth_response or self._auth_response()
        if not auth_response.ok:
            raise AnaplanAuthException(
                f"Authentication against Anaplan failed: {auth_response.text}"
            )
        token_value = auth_response.json()["tokenInfo"]["tokenValue"]

        return {
            "Authorization": f"AnaplanAuthToken {token_value}",
            "Content-Type": "application/json",
        }


class AnaplanTap(Tap):

    def __init__(
        self,
        workspaceID,
        modelID,
        exportID,
        fileID,
        username,
        password,
        integration_service: Optional[AnaplanIntegrationService] = None,
    ):
        self.integration_service = integration_service or AnaplanIntegrationService(
            workspaceID=workspaceID,
            modelID=modelID,
            exportID=exportID,
            fileID=fileID,
            username=username,
            password=password,
        )

    def column_descriptions(self) -> list[Description]:
        export = self.integration_service.export_information()
        column_names = export["exportMetadata"]["headerNames"]
        data_types = export["exportMetadata"]["dataTypes"]
        metadata = zip(column_names, data_types)
        descriptions = []
        for elem in metadata:
            desc = Description(
                name=elem[0], type=elem[1], precision=None, scale=None, nullable=True
            )
            descriptions.append(desc)

        return descriptions

    def data_generator(
        self,
    ) -> Generator[list[tuple], Any, None]:
        trigger_response = self.integration_service.trigger_export_task()
        taskID = trigger_response["task"]["taskId"]

        while (
            self.integration_service.export_task_status(taskID=taskID)["task"][
                "taskState"
            ]
            != "COMPLETE"
        ):
            time.sleep(1)

        file_chunks_response = self.integration_service.number_of_file_chunks()
        file_chunks = file_chunks_response.get("chunks") or [{"id": "0"}]
        file = bytes()
        for chunk in file_chunks:
            chunkID = chunk["id"]
            file = file + self.integration_service.file_chunk(chunkID=chunkID)
        bytes_to_file = io.StringIO(file.decode("utf-8"))
        data = []
        with bytes_to_file as f:
            reader = csv.reader(f)

            # Skip header
            next(reader, None)

            for row in reader:
                data.append(tuple(row))

        yield data
