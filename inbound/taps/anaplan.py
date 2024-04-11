import base64
import csv
import io
import json
import time
from typing import Any, Generator

import requests

from inbound.core.models import Description
from inbound.sdk.tap import Tap


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

    # TODO: Må refaktoreres
    def column_descriptions(self) -> list[Description]:
        # TODO: Isoler IO
        import_headers = self._get_header()
        url = f"https://api.anaplan.com/2/0/workspaces/{self.workspaceID}/models/{self.modelID}/exports/{self.exportID}"
        respons = requests.get(
            url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        # TODO: Dette bør være responsen fra isolert IO
        export_respons = respons.json()

        column_names = export_respons["exportMetadata"]["headerNames"]
        data_types = export_respons["exportMetadata"]["dataTypes"]
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
        header = self._get_header()

        taskID = self._import_data(
            header, self.workspaceID, self.modelID, self.exportID
        )
        task_status = self._sjekk_status(
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

    # TODO: Må refaktoreres
    def _get_header(self):
        # TODO: Bør isoleres som en input IO til anaplan
        token_url = "https://auth.anaplan.com/token/authenticate"
        user = "Basic " + str(
            base64.b64encode(
                (f"{self.username}:{self.password}").encode("utf-8")
            ).decode("utf-8")
        )
        token_headers = {"Authorization": user, "Content-Type": "application/json"}

        # TODO: Isoler IO
        token = requests.post(
            url=token_url,
            headers=token_headers,
            data=json.dumps({"localeName": "en_US"}),
        )
        # TODO: Response fra isolert IO
        token_value = token.json()["tokenInfo"]["tokenValue"]

        # TODO: Bør isoleres som en response
        import_headers = {
            "Authorization": f"AnaplanAuthToken {token_value}",
            "Content-Type": "application/json",
        }

        return import_headers

    # IO mot anaplan
    def get_auth_token(self, token_url, token_header) -> str: ...

    def auth_header(self, auth_token) -> dict: ...

    def _import_data(self, header, workspaceID, modelID, exportID):

        import_headers = header

        import_url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/exports/{exportID}/tasks"

        postImport = requests.post(
            import_url, headers=import_headers, data=json.dumps({"localeName": "en_US"})
        )
        # print(json.dumps(postImport.json(), indent=2))
        taskID = postImport.json()["task"]["taskId"]

        return taskID

    def _sjekk_status(self, header, workspaceID, modelID, exportID, taskID):

        import_headers = header

        status_url = f"https://api.anaplan.com/2/0/workspaces/{workspaceID}/models/{modelID}/exports/{exportID}/tasks/{taskID}"
        print(f"status url: {status_url}")
        while True:
            respons = requests.get(
                status_url,
                headers=import_headers,
                data=json.dumps({"localeName": "en_US"}),
            )
            # print(json.dumps(respons.json(), indent=2))
            task_status = respons.json()["task"]["taskState"]
            # print(task_status)
            if task_status == "COMPLETE":
                break
            time.sleep(1)

        return task_status

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
