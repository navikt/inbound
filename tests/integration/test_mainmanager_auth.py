import os
from unittest import TestCase

from inbound.taps.mainmanager import MainManagerError, MainManagerTap


class TestMainManager(TestCase):

    def test_mainmanager_auth_ok(self):
        tap = MainManagerTap(
            table="dim_bygg_01",
            username=os.environ["MAINMANAGER_API_USERNAME"],
            password=os.environ["MAINMANAGER_API_PASSWORD"],
        )

        datadump = next(tap.data_generator())

        assert datadump is not None

    def test_mainmanager_should_raise_exception_if_table_not_exists(self):
        tap = MainManagerTap(
            table="blabla",
            username=os.environ["MAINMANAGER_API_USERNAME"],
            password=os.environ["MAINMANAGER_API_PASSWORD"],
        )

        with self.assertRaises(MainManagerError):
            datadump = next(tap.data_generator())

    def test_mainmanager_should_raise_exception_if_wrong_username(self):
        tap = MainManagerTap(
            table="dim_bygg_01",
            username="blabla",
            password=os.environ["MAINMANAGER_API_PASSWORD"],
        )

        with self.assertRaises(MainManagerError):
            datadump = next(tap.data_generator())

    def test_mainmanager_should_raise_exception_if_wrong_password(self):
        tap = MainManagerTap(
            table="dim_bygg_01",
            username=os.environ["MAINMANAGER_API_USERNAME"],
            password="blabla",
        )

        with self.assertRaises(MainManagerError):
            datadump = next(tap.data_generator())
