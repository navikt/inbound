import dataclasses
import json
from datetime import datetime

from inbound.core.models import Description, Metadata
from inbound.sdk.mapper import Mapper
from inbound.sdk.sink import Sink
from inbound.sdk.tap import Tap


class Job:
    def __init__(
        self,
        tap: Tap,
        sink: Sink,
        description_mapper: Mapper = None,
        metadata: Metadata = None,
        raw: bool = False,
    ) -> None:
        self.tap = tap
        self.sink = sink
        self.mapper = description_mapper
        self.metadata = metadata
        self.raw = raw

    def metadata_generator(
        self, data_generator, column_description: list[Description] = None
    ):
        if column_description is None:
            for data in data_generator:
                yield [tuple(row) + dataclasses.astuple(self.metadata) for row in data]
        else:
            desc = tuple(row.name for row in column_description)
            for data in data_generator:
                yield [
                    row
                    + (json.dumps(dict(zip(desc, row)), default=str),)
                    + dataclasses.astuple(self.metadata)
                    for row in data
                ]

    def run(self):
        run_start = datetime.now()
        if self.metadata:
            run_start = self.metadata.load_time

        tap_data_generator = self.tap.data_generator()
        tap_desc = self.tap.column_descriptions()

        sink_desc = tap_desc
        if self.mapper is not None:
            sink_desc = [self.mapper.map(desc) for desc in tap_desc]

        if self.metadata is not None and not self.raw:
            sink_desc.extend(self.metadata.get_description())
            tap_data_generator = self.metadata_generator(tap_data_generator)

        if self.raw:
            sink_desc.append(
                Description(
                    name="_inbound__raw",
                    type="variant",
                    precision=None,
                    scale=None,
                    nullable=False,
                )
            )
            sink_desc.extend(self.metadata.get_description())
            tap_data_generator = self.metadata_generator(
                tap_data_generator, column_description=sink_desc
            )

        batch_results = self.sink.ingest(
            data_generator=tap_data_generator, column_description=sink_desc
        )

        return {
            "tap": self.tap.__class__.__name__,
            "sink": self.sink.__class__.__name__,
            "start": run_start,
            "stop": datetime.now(),
            "columns": [col.name for col in sink_desc],
            "batches": batch_results,
        }
