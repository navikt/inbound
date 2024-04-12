from typing import Any, Generator

from ..core.models import Description
from ..sdk.sink import Sink


class CsvSink(Sink):
    def __init__(self, csv_writer):
        self.csv_writer = csv_writer

    def ingest(
        self,
        data_generator: Generator[list[tuple], Any, None],
        column_description: list[Description],
    ):
        col_names = tuple(c.name for c in column_description)
        self.csv_writer.writerow(col_names)
        for data in data_generator:
            self.csv_writer.writerows(data)
