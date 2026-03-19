import time
from unittest import TestCase
from unittest.mock import MagicMock

import requests

from inbound.core.models import Description
from inbound.taps.anaplan import (
    AnaplanAuthException,
    AnaplanIntegrationService,
    AnaplanTap,
)


class DummyIntegrationService(AnaplanIntegrationService):
    def __init__(self):
        super().__init__(
            workspaceID="", modelID="", exportID="", fileID="", username="", password=""
        )

    def trigger_export_task(self):
        return {"task": {"taskId": "foo"}}

    def export_task_status(self, taskID):
        return {"task": {"taskState": "COMPLETE"}}

    def number_of_file_chunks(self):
        return {}

    def file_chunk(self, chunkID):
        return b"header\nfirst\n"

    def export_information(self): ...
    def _auth_response(self): ...
    def _headers(self, auth_response=None): ...


tap = AnaplanTap(
    workspaceID="",
    modelID="",
    exportID="",
    fileID="",
    username="",
    password="",
    integration_service=DummyIntegrationService(),
)


class TestAnaplan(TestCase):

    def test_auth_response_status_code_under_400_returns_header(self):
        auth_response_content = {
            "meta": {"validationUrl": "https://auth.anaplan.com/token/validate"},
            "status": "SUCCESS",
            "statusMessage": "Login successful",
            "tokenInfo": {
                "expiresAt": 1,
                "tokenId": "foo",
                "tokenValue": "bar",
                "refreshTokenId": "baz",
            },
        }
        auth_response = requests.Response()
        auth_response.status_code = 201
        auth_response.json = MagicMock(return_value=auth_response_content)
        result = AnaplanIntegrationService(
            workspaceID="", modelID="", exportID="", fileID="", username="", password=""
        )._headers(auth_response=auth_response)

        expected = {
            "Authorization": "AnaplanAuthToken bar",
            "Content-Type": "application/json",
        }
        assert expected == result

    def test_auth_response_status_code_above_400_raises_an_exception(self):

        auth_response = requests.Response()
        auth_response.status_code = 401

        with self.assertRaises(AnaplanAuthException) as cm:
            AnaplanIntegrationService(
                workspaceID="",
                modelID="",
                exportID="",
                fileID="",
                username="",
                password="",
            )._headers(auth_response=auth_response)

    def test_data_generator_defaults_to_chunk_id_0(self):

        class DefaultsToChunkId0(DummyIntegrationService):
            def file_chunk(self, chunkID):
                assert chunkID == "0"
                return bytes()

        tap = AnaplanTap(
            workspaceID="",
            modelID="",
            exportID="",
            fileID="",
            username="",
            password="",
            integration_service=DefaultsToChunkId0(),
        )

        while next(tap.data_generator()):
            ...

    def test_data_generator_one_chunk(self):
        result = [data for data in tap.data_generator()]
        expected = [[("first",)]]
        assert result == expected

    def test_data_generator_multi_chunk_ids(self):
        class Multichunks(DummyIntegrationService):
            def number_of_file_chunks(self):
                return {"chunks": [{"id": "0"}, {"id": "1"}]}

            def file_chunk(self, chunkID):
                if chunkID == "0":
                    return b"header\n"
                if chunkID == "1":
                    return b"second\n"
                raise Exception("chunkID should only be 0 or 1:", chunkID)

        tap = AnaplanTap(
            workspaceID="",
            modelID="",
            exportID="",
            fileID="",
            username="",
            password="",
            integration_service=Multichunks(),
        )

        result = [data for data in tap.data_generator()]
        expected = [[("second",)]]
        assert result == expected

    def test_data_generator_is_waiting_if_export_task_is_not_ready(self):
        class Waiting(DummyIntegrationService):
            def __init__(self):
                super().__init__()
                self.iterations = 0

            def export_task_status(self, taskID):
                if self.iterations == 0:
                    self.iterations = self.iterations + 1
                    return {"task": {"taskState": "foo"}}
                return {"task": {"taskState": "COMPLETE"}}

        tap = AnaplanTap(
            workspaceID="",
            modelID="",
            exportID="",
            fileID="",
            username="",
            password="",
            integration_service=Waiting(),
        )

        start_time = time.perf_counter()
        result = [data for data in tap.data_generator()]
        end_time = time.perf_counter()
        result = end_time - start_time
        expected = 1
        assert result > expected

    def test_column_description(self):
        class ExportService(DummyIntegrationService):
            def export_information(self):
                return {
                    "exportMetadata": {
                        "headerNames": ["foo", "bar", "baz"],
                        "dataTypes": ["footype", "bartype", "baztype"],
                    },
                }

        tap = AnaplanTap(
            workspaceID="",
            modelID="",
            exportID="",
            fileID="",
            username="",
            password="",
            integration_service=ExportService(),
        )

        result = tap.column_descriptions()
        expected = [
            Description(
                name="foo", type="footype", precision=None, scale=None, nullable=True
            ),
            Description(
                name="bar", type="bartype", precision=None, scale=None, nullable=True
            ),
            Description(
                name="baz", type="baztype", precision=None, scale=None, nullable=True
            ),
        ]
        assert result == expected
