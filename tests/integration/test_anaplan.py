import os
from unittest import TestCase

from inbound.core.models import Description
from inbound.taps.anaplan import AnaplanTap

workspaceID = "8a868cd985f53e7701860542f59e276e"
modelID = "7C773072370243ED9FDD224F0A3B6E53"
exportID = "116000000001"
fileID = "116000000001"

username = os.environ["ANAPLAN_USR"]
password = os.environ["ANAPLAN_PWD"]


class TestAnaplan(TestCase):

    def test_column_description(self):

        tap = AnaplanTap(
            workspaceID=workspaceID,
            modelID=modelID,
            exportID=exportID,
            fileID=fileID,
            username=username,
            password=password,
        )
        result = tap.column_descriptions()
        expected = Description(
            name="export_code", type="TEXT", precision=None, scale=None, nullable=True
        )

        assert result[0] == expected

    def test_data_generator(self):
        tap = AnaplanTap(
            workspaceID=workspaceID,
            modelID=modelID,
            exportID=exportID,
            fileID=fileID,
            username=username,
            password=password,
        )
        result = []
        for row in tap.data_generator():
            result.append(row)
        expected = (
            "162100-060501-000000-000000-CB01-2024_01",
            "162100",
            "060501",
            "000000",
            "000000",
            "CB01",
            "2024_01",
            "",
            "false",
            "0",
        )

        assert result[0][0] == expected

