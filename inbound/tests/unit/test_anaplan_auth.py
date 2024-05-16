from unittest import TestCase
from unittest.mock import MagicMock

import requests

from inbound.taps.anaplan import AnaplanAuthException, AnaplanTap

tap = AnaplanTap(
    workspaceID="",
    modelID="",
    exportID="",
    fileID="",
    username="",
    password="password",
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
