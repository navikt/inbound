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
        url = f"https://api.anaplan.com/2/0/workspaces/{self.workspaceID}/models/{self.modelID}/exports/{self.exportID}"
        respons = requests.get(
            url, headers=import_header, data=json.dumps({"localeName": "en_US"})
        )
        return respons.json()

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

    # TODO: Må refaktoreres
    def data_generator(self) -> Generator[list[tuple], Any, None]:
        # TODO: Isoler IO
        auth_response = self._get_auth_response()
        header = self._get_header(auth_response=auth_response)
        taskID = self._import_data(
            header, self.workspaceID, self.modelID, self.exportID
        )
        self._check_status(
            header, self.workspaceID, self.modelID, self.exportID, taskID
        )
        chunks = self._get_file_chunks(
            header, self.workspaceID, self.modelID, self.fileID
        )
        file = bytes()
        for chunk in chunks:
            chunkID = chunk["id"]
            # TODO: Isoler IO
            file = file + self._get_exported_file_chunk(
                header, self.workspaceID, self.modelID, self.fileID, chunkID
            )
        bytes_to_file = io.StringIO(file.decode("utf-8"))
        data = []
        with bytes_to_file as f:
            reader = csv.reader(f)

            # Skip header
            next(reader, None)

            for row in reader:
                data.append(tuple(row))

        yield data

    def _get_auth_response(self):
        user = "Basic " + str(
            base64.b64encode(
                (f"{self.username}:{self.password}").encode("utf-8")
            ).decode("utf-8")
        )
        auth_header = {"Authorization": user, "Content-Type": "application/json"}

        auth_response = requests.post(
            url=self.auth_url,
            headers=auth_header,
            data=json.dumps({"localeName": "en_US"}),
        )
        return auth_response

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

    def _import_data(self, header, workspaceID, modelID, exportID):
        import_headers = header
        import_url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/exports/{exportID}/tasks"
        post_import = requests.post(
            import_url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        taskID = post_import.json()["task"]["taskId"]
        return taskID

    def _check_status(self, header, workspaceID, modelID, exportID, taskID):
        import_headers = header
        status_url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/exports/{exportID}/tasks/{taskID}"
        print(f"status url: {status_url}")
        while True:
            respons = requests.get(
                status_url,
                headers=import_headers,
                data=json.dumps({"localeName": "en_US"}),
            )
            task_status = respons.json()["task"]["taskState"]
            if task_status == "COMPLETE":
                break
            time.sleep(1)

    # IO mot anaplan. Ok
    def _get_file_chunks(self, header, workspaceID, modelID, fileID):
        import_headers = header
        url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/files/{fileID}/chunks/"
        respons = requests.get(
            url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        # Test om chunks eksisterer
        # chunks er en liste av dictionaries hvor hver dictionary har en chunk ID
        try:
            chunks = respons.json()["chunks"]
        except:
            chunks = [{"id": "0"}]  # ingen chunks, kun en fil
        return chunks

    # IO mot anaplan. Ok
    def _get_exported_file_chunk(self, header, workspaceID, modelID, fileID, chunkID):
        import_headers = header
        url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/files/{fileID}/chunks/{chunkID}"
        respons = requests.get(
            url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        return respons.content
