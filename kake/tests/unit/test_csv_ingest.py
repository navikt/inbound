import csv
import datetime
import io
from unittest import TestCase

from kake.core.models import Description
from kake.sinks.csv import CsvSink


class TestCsvIngest(TestCase):
    def test_csv_ingest(self):
        with io.StringIO(newline="") as f:

            def gen():
                yield [(1,), (2,)]

            desc = [
                Description(
                    name="foo",
                    type="number",
                    precision=38,
                    scale=0,
                    nullable=True,
                )
            ]
            writer = csv.writer(f)
            sink = CsvSink(csv_writer=writer)
            sink.ingest(data_generator=gen(), column_description=desc)

            expected = "foo\r\n1\r\n2\r\n"
            result = f.getvalue()
            assert expected == result
