from datetime import datetime
from typing import Any, Generator
from unittest import TestCase

from inbound.core.job import Job
from inbound.core.models import Description, Metadata
from inbound.sdk.sink import Sink
from inbound.sdk.tap import Tap


class MockTap(Tap):
    def column_descriptions(self) -> list[Description]:
        return [
            Description(
                name="foo", type=None, precision=None, scale=None, nullable=True
            )
        ]

    def data_generator(self) -> Generator[list[tuple], Any, None]:
        yield [(1,)]


class MockSink(Sink):
    def __init__(self) -> None:
        self.captured_result = []
        self.capured_column_description = None

    def ingest(
        self,
        data_generator: Generator[list[tuple], Any, None],
        column_description: list[Description],
    ):
        self.capured_column_description = column_description
        for i in data_generator:
            self.captured_result.append(i)


metadata = Metadata(
    load_time=datetime(2020, 1, 1),
    source_env="foo",
    run_id="bar",
    job_name="baz",
    inbound_version="foobar",
)


class TestJob(TestCase):
    def test_adding_metadata_should_generate_more_data(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        job.run()
        result = sink.captured_result
        expected = [[(1, "foo", "bar", "baz", "foobar", datetime(2020, 1, 1, 0, 0))]]
        assert result == expected

    def test_job_result_tap_name(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        result = job.run()["tap"]
        print(result)
        expected = "MockTap"
        assert result == expected

    def test_job_result_sink_name(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        result = job.run()["sink"]
        print(result)
        expected = "MockSink"
        assert result == expected

    def test_job_result_columns(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        result = job.run()["columns"]
        print(result)
        expected = [
            "foo",
            "_inbound__source_env",
            "_inbound__run_id",
            "_inbound__job_name",
            "_inbound__version",
            "_inbound__load_time",
        ]
        assert result == expected

    def test_job_result_start(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        result = job.run()["start"]
        print(result)
        expected = metadata.load_time
        assert result == expected

    def test_job_result_stop_is_greater_than_start(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        stop = job.run()["stop"]
        print(stop)
        start = metadata.load_time
        assert stop > start

    def test_adding_metadata_should_generate_more_column_descriptions(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata)
        job.run()
        result = sink.capured_column_description
        expected = [
            Description(
                name="foo", type=None, precision=None, scale=None, nullable=True
            ),
            Description(
                name="_inbound__source_env",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__run_id",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__job_name",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__version",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__load_time",
                type="timestamp",
                precision=None,
                scale=None,
                nullable=False,
            ),
        ]
        assert result == expected

    def test_not_metadata_should_not_generate_more_data(self):
        tap = MockTap()
        sink = MockSink()

        job = Job(tap=tap, sink=sink)
        job.run()
        result = sink.captured_result
        expected = [[(1,)]]
        assert result == expected

    def test_not_adding_metadata_should_not_generate_more_column_descriptions(self):
        tap = MockTap()
        sink = MockSink()

        job = Job(tap=tap, sink=sink)
        job.run()
        result = sink.capured_column_description
        expected = [
            Description(
                name="foo", type=None, precision=None, scale=None, nullable=True
            )
        ]
        assert result == expected

    def test_adding_metadata_with_raw_should_generate_raw_data(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata, raw=True)
        job.run()
        result = sink.captured_result
        expected = [
            [
                (
                    1,
                    '{"foo": 1}',
                    "foo",
                    "bar",
                    "baz",
                    "foobar",
                    datetime(2020, 1, 1, 0, 0),
                )
            ]
        ]
        assert result == expected

    def test_adding_metadata_raw_should_generate_raw_column_descriptions(self):
        tap = MockTap()
        sink = MockSink()
        job = Job(tap=tap, sink=sink, metadata=metadata, raw=True)
        job.run()
        result = sink.capured_column_description
        expected = [
            Description(
                name="foo", type=None, precision=None, scale=None, nullable=True
            ),
            Description(
                name="_inbound__raw",
                type="variant",
                precision=None,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__source_env",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__run_id",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__job_name",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__version",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__load_time",
                type="timestamp",
                precision=None,
                scale=None,
                nullable=False,
            ),
        ]
        assert result == expected

    def test_metadata_load_time_should_update(self):
        meta1 = Metadata(source_env="foo", run_id="bar", job_name="baz")
        meta2 = Metadata(source_env="foo", run_id="bar", job_name="baz")
        assert meta1.load_time != meta2.load_time
