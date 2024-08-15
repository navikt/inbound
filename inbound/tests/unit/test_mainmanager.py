import csv
import datetime
import io
from unittest import TestCase

from inbound.core.models import Description
from inbound.sinks.csv import CsvSink
from inbound.taps.mainmanager import MainManagerTap


class DummySupplier:
    def get_data(self):
        return {"DataTable": '{"table":[{"test":"wohoo"}, {"test2":"jippi"}]}'}


tap = MainManagerTap(table="", username="", password="", data_supplier=DummySupplier())


class TestMainManager(TestCase):
    def test_mainmanager_data_generator_supplies_batches_of_data(self):
        result = next(tap.data_generator())
        expected = [('{"test": "wohoo"}',), ('{"test2": "jippi"}',)]

        assert result == expected

    def test_column_descriptions(self):
        result = tap.column_descriptions()
        expected = [
            Description(
                name="raw", type="variant", precision=None, scale=None, nullable=True
            )
        ]

        assert result == expected
