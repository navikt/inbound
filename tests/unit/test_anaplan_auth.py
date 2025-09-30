import time
from dataclasses import dataclass
from unittest import TestCase
from unittest.mock import MagicMock

import requests

from inbound.core.models import Description
from inbound.taps.anaplan import AnaplanAuthException, AnaplanTap

tap = AnaplanTap(
    workspaceID="",
    modelID="",
    exportID="",
    fileID="",
    username="",
    password="",
)


@dataclass
class AuthService(requests.Response):
    ok = True

    def json(self):
        return {"tokenInfo": {"tokenValue": "foo"}}


def trigger_export_service(header):
    return {"task": {"taskId": "foo"}}


def export_task_status_service(header, taskID):
    return {"task": {"taskState": "COMPLETE"}}


def number_of_file_chunks_service():
    return {}


def file_chunk_service(header, chunkID):
    return b"header\nfirst\n"


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

        result = tap._get_header(auth_response=auth_response)
        expected = {
            "Authorization": "AnaplanAuthToken bar",
            "Content-Type": "application/json",
        }
        assert expected == result

    def test_auth_response_status_code_above_400_raises_an_exception(self):

        auth_response = requests.Response()
        auth_response.status_code = 401

        with self.assertRaises(AnaplanAuthException) as cm:
            tap._get_header(auth_response=auth_response)

    def test_data_generator_defaults_to_chunk_id_0(self):

        def file_chunk_service(header, chunkID):
            assert chunkID == "0"
            return bytes()

        for data in tap.data_generator(
            auth_service=AuthService,
            trigger_export_service=trigger_export_service,
            export_task_status_service=export_task_status_service,
            number_of_file_chunks_service=number_of_file_chunks_service,
            file_chunk_service=file_chunk_service,
        ):
            continue

    def test_data_generator_one_chunk(self):
        result = [
            data
            for data in tap.data_generator(
                auth_service=AuthService,
                trigger_export_service=trigger_export_service,
                export_task_status_service=export_task_status_service,
                number_of_file_chunks_service=number_of_file_chunks_service,
                file_chunk_service=file_chunk_service,
            )
        ]
        expected = [[("first",)]]
        assert result == expected

    def test_data_generator_multi_chunk_ids(self):
        def number_of_file_chunks_service():
            return {"chunks": [{"id": "0"}, {"id": "1"}]}

        def file_chunk_service(header, chunkID):
            if chunkID == "0":
                return b"header\n"
            if chunkID == "1":
                return b"first\n"
            raise Exception("chunkID should only be 0 or 1:", chunkID)

        result = [
            data
            for data in tap.data_generator(
                auth_service=AuthService,
                trigger_export_service=trigger_export_service,
                export_task_status_service=export_task_status_service,
                number_of_file_chunks_service=number_of_file_chunks_service,
                file_chunk_service=file_chunk_service,
            )
        ]
        expected = [[("first",)]]
        assert result == expected

    def test_data_generator_is_waiting_if_export_task_is_not_ready(self):
        global iterations
        iterations = 0

        def export_task_status_service(header, taskID):
            global iterations
            if iterations == 0:
                iterations = iterations + 1
                return {"task": {"taskState": "foo"}}
            return {"task": {"taskState": "COMPLETE"}}

        start_time = time.perf_counter()
        result = [
            data
            for data in tap.data_generator(
                auth_service=AuthService,
                trigger_export_service=trigger_export_service,
                export_task_status_service=export_task_status_service,
                number_of_file_chunks_service=number_of_file_chunks_service,
                file_chunk_service=file_chunk_service,
            )
        ]
        end_time = time.perf_counter()
        result = end_time - start_time
        expected = 1
        assert result > expected

    def test_column_description(self):
        def export_column_description_service():
            return {
                "exportMetadata": {
                    "headerNames": ["foo", "bar", "baz"],
                    "dataTypes": ["footype", "bartype", "baztype"],
                },
            }

        result = tap.column_descriptions(
            export_info_service=export_column_description_service
        )
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
