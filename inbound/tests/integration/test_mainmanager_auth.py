import os
from unittest import TestCase

from inbound.core.models import Description
from inbound.taps.mainmanager import MainManagerTap

tap = MainManagerTap(
            table="dim_bygg_01",
            username = os.environ["MAINMANAGER_API_USERNAME"],          
            password = os.environ["MAINMANAGER_API_PASSWORD"],
        )


class TestMainManager(TestCase):

    def test_get_mainmanager_token_returns_token(self):
        token = tap._get_mainmanager_token()
        assert token is not None

    def test_column_descriptions(self):
        result = tap.column_descriptions()
        expected = [
            Description(
                name="raw", type="variant", precision=None, scale=None, nullable=True
            )
        ]

        assert result == expected

    # TODO: Få satt opp enkelt API-endepunkt i MainManager vi kan spørre mot
    def test_data_generator(self):
        result = [rows for rows in tap.data_generator()]
        expected = ""

        assert result == expected

    
