import base64
import csv
import io
import json
import time
from typing import Any, Callable, Generator

import requests

from ..core.models import Description
from ..sdk.tap import Tap


class AnaplanAuthException(Exception):
    pass


class AnaplanTap(Tap):

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

    def _get_export_response(self) -> dict:
        auth_response = self._get_auth_response()
        import_header = self._get_header(auth_response=auth_response)
        url = f"{self.base_url}/exports/{self.exportID}"
        respons = requests.get(
            url, headers=import_header, data=json.dumps({"localeName": "en_US"})
        )
        return respons.json()

    def _get_auth_response(self) -> requests.Response:
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

    def _trigger_export(self, header) -> dict:
        url = f"{self.base_url}/exports/{self.exportID}/tasks"
        return requests.post(
            url, headers=header, data=json.dumps({"localeName": "en_US"})
        ).json()

    def _check_export_task_status(self, header, taskID) -> dict:
        status_url = f"{self.base_url}/exports/{self.exportID}/tasks/{taskID}"
        return requests.get(
            status_url,
            headers=header,
            data=json.dumps({"localeName": "en_US"}),
        ).json()

    def _get_number_of_file_chunks(self, header) -> dict:
        import_headers = header
        url = f"{self.base_url}/files/{self.fileID}/chunks/"
        return requests.get(
            url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        ).json()

    def _get_file_chunk(self, header, chunkID) -> bytes:
        import_headers = header
        url = f"{self.base_url}/files/{self.fileID}/chunks/{chunkID}"
        respons = requests.get(
            url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        return respons.content

    def column_descriptions(
        self, export_info_service: Callable = _get_export_response
    ) -> list[Description]:
        response_json = export_info_service()

        column_names = response_json["exportMetadata"]["headerNames"]
        data_types = response_json["exportMetadata"]["dataTypes"]
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
        auth_service: Callable = _get_auth_response,
        trigger_export_service: Callable = _trigger_export,
        export_task_status_service: Callable = _check_export_task_status,
        number_of_file_chunks_service: Callable = _get_number_of_file_chunks,
        file_chunk_service: Callable = _get_file_chunk,
    ) -> Generator[list[tuple], Any, None]:
        auth_response = auth_service()
        header = self._get_header(auth_response=auth_response)
        trigger_export_response = trigger_export_service(header=header)
        taskID = trigger_export_response["task"]["taskId"]
        while True:
            export_task_status_response = export_task_status_service(
                header=header, taskID=taskID
            )
            task_status = export_task_status_response["task"]["taskState"]
            if task_status == "COMPLETE":
                break
            time.sleep(1)
        file_chunks_response = number_of_file_chunks_service()
        chunks = file_chunks_response.get("chunks") or [{"id": "0"}]
        file = bytes()
        for chunk in chunks:
            chunkID = chunk["id"]
            file = file + file_chunk_service(header=header, chunkID=chunkID)
        bytes_to_file = io.StringIO(file.decode("utf-8"))
        data = []
        with bytes_to_file as f:
            reader = csv.reader(f)

            # Skip header
            next(reader, None)

            for row in reader:
                data.append(tuple(row))

        yield data

    def _get_header(self, auth_response: requests.Response):
        if not auth_response.ok:
            raise AnaplanAuthException(
                f"Authentication against Anaplan failed: {auth_response.text}"
            )
        token_value = auth_response.json()["tokenInfo"]["tokenValue"]
        import_headers = {
            "Authorization": f"AnaplanAuthToken {token_value}",
            "Content-Type": "application/json",
        }
        return import_headers
