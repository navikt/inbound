import os
from unittest import TestCase

from inbound.taps.mainmanager import MainManagerTap

tap = MainManagerTap(
            table="",
            username = os.environ["MAINMANAGER_API_USERNAME"],          
            password = os.environ["MAINMANAGER_API_PASSWORD"],
        )


class TestMainManager(TestCase):

    def test_get_mainmanager_token_returns_token(self):
        token = tap._get_mainmanager_token()
        assert token is not None
    
