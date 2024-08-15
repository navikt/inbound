import csv
import datetime
import io
from unittest import TestCase

from inbound.core.models import Description
from inbound.sinks.csv import CsvSink
from inbound.taps.mainmanager import MainManagerTap


class TestMainManager(TestCase):
    def test_mainmanager_data_generator_supplies_batches_of_data(self):
        class DummySupplier():
            def get_data(self):
                return {"DataTable": '{"table":[{"test":"wohoo"}, {"test2":"jippi"}]}'}
            
        tap = MainManagerTap(table='', username='', password='', data_supplier=DummySupplier())

        result = None
        for item in tap.data_generator():
            result = item
            break

        expected = [{"test":"wohoo"}, {"test2":"jippi"}]

        assert result == expected
