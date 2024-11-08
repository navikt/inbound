from datetime import datetime
from io import StringIO
from unittest import TestCase

from inbound.core.models import Description
from inbound.sinks.snowflake import SnowSink


class MockSnowHandler:
    def create_table(self, ddl: str): ...
    def ingest_file_to_table(
        self,
        table: str,
        database: str,
        schema: str,
        file_path: str,
        file_name: str,
    ): ...
    def ingest_from_table(self, table, to_table): ...
    def drop_table(self, table): ...
    def swap_tables(self, old_table, new_table): ...


class MockFileHandler:
    def __init__(self, file_path: str = "/tmp/inbound", file_name: str = "inbound.csv"):
        self.file_path = file_path
        self.file_name = file_name
        self.full_file_name = f"{file_path}/{file_name}"

    def create_file(self):
        return StringIO(newline="")

    def create_dir(self): ...
    def close_file(self, file): ...
    def delete_file(self): ...


def mock_generator():
    yield [(0,)]


mock_desc = [
    Description(name=None, type=None, precision=None, scale=None, nullable=True)
]


class TestSnowSink(TestCase):
    def test_create_ddl_number(self):
        desc = [
            Description(
                name="foo",
                type="number",
                precision=38,
                scale=0,
                nullable=True,
            )
        ]
        result = SnowSink.create_ddl(
            table="this.that.foo",
            column_descriptions=desc,
            transient=False,
        )
        expected = "create table if not exists this.that.foo (foo number(38, 0))"
        assert result.strip() == expected.strip()

    def test_create_ddl_varchar_should_not_have_precision_or_scale(self):
        desc = [
            Description(
                name="foo",
                type="varchar",
                precision=38,
                scale=5,
                nullable=True,
            )
        ]
        result = SnowSink.create_ddl(
            table="this.that.foo",
            column_descriptions=desc,
            transient=False,
        )
        expected = "create table if not exists this.that.foo (foo varchar)"
        assert result.strip() == expected.strip()

    def test_create_ddl_number_with_scale(self):
        desc = [
            Description(
                name="bar",
                type="number",
                precision=38,
                scale=2,
                nullable=True,
            )
        ]
        result = SnowSink.create_ddl(
            table="foo",
            column_descriptions=desc,
            transient=False,
        )
        expected = "create table if not exists foo (bar number(38, 2))"
        assert result.strip() == expected.strip()

    def test_tmp_file_max_size(self):
        class MockFileHandler:
            def __init__(
                self, file_path: str = "/tmp/inbound", file_name: str = "inbound.csv"
            ):
                self.file_path = file_path
                self.file_name = file_name
                self.full_file_name = f"{file_path}/{file_name}"

            file = None
            files_created = 0
            ingested_data = []

            def create_dir(self): ...
            def create_file(self):
                self.file = StringIO(newline="")
                self.files_created = self.files_created + 1
                return self.file

            def close_file(self, file):
                self.ingested_data.append(file.getvalue())
                file.close()

            def delete_file(self): ...

        def generator():
            yield [[(0)]]

        desc = [
            Description(
                name="a",
                type="number",
                precision=38,
                scale=0,
                nullable=True,
            )
        ]
        file_handler = MockFileHandler()
        sink = SnowSink(
            connection_handler=MockSnowHandler(),
            file_handler=file_handler,
            table="foo.bar.baz",
            transient=False,
            tmp_file_max_size=4,
        )
        sink.ingest(data_generator=generator(), column_description=desc)

        result_files_created = file_handler.files_created
        expected_files_created = 2
        assert result_files_created == expected_files_created

        result_ingested_data = file_handler.ingested_data
        expected_ingested_data = ["a\r\n0\r\n", "a\r\n"]
        assert result_ingested_data == expected_ingested_data

    def test_batch_result_number(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
        )
        run_result = sink.ingest(
            data_generator=mock_generator(), column_description=mock_desc
        )
        run_result_len = len(run_result)
        expected_run_result_len = 1
        assert run_result_len == expected_run_result_len

        result = run_result[0]["number"]
        expected = 0
        assert result == expected

    def test_batch_result_stop_is_greater_than_start(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
        )
        run_result = sink.ingest(
            data_generator=mock_generator(), column_description=mock_desc
        )
        run_result_len = len(run_result)
        expected_run_result_len = 1
        assert run_result_len == expected_run_result_len

        start = run_result[0]["start"]
        stop = run_result[0]["stop"]
        assert stop > start

    def test_multiple_batches_number(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
            tmp_file_max_size=4,
        )
        run_result = sink.ingest(
            data_generator=mock_generator(), column_description=mock_desc
        )
        run_result_len = len(run_result)
        expected_run_result_len = 2
        assert run_result_len == expected_run_result_len

        result = run_result[1]["number"]
        expected = 1
        assert result == expected

    def test_batch_result_row(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
        )

        def mock_generator():
            for i in range(2):
                yield [(0,), (1,)]

        run_result = sink.ingest(
            data_generator=mock_generator(), column_description=mock_desc
        )
        result = run_result[0]["rows"]
        expected = 4
        assert result == expected

    def test_run_result_includes_batch_result(self): ...  # TODO

    def test_empty_data_generator(self):

        class MockFileHandler:
            def __init__(
                self, file_path: str = "/tmp/inbound", file_name: str = "inbound.csv"
            ):
                self.file_path = file_path
                self.file_name = file_name
                self.full_file_name = f"{file_path}/{file_name}"

            file = None
            files_created = 0
            ingested_data = []

            def create_dir(self): ...
            def create_file(self):
                self.file = StringIO(newline="")
                self.files_created = self.files_created + 1
                return self.file

            def close_file(self, file):
                self.ingested_data.append(file.getvalue())
                file.close()

            def delete_file(self): ...

        file_handler = MockFileHandler()
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=file_handler,
        )

        def mock_generator():
            return
            yield "foo"

        run_result = sink.ingest(
            data_generator=mock_generator(), column_description=mock_desc
        )

        run_result_len = len(run_result)
        expected_run_result_len = 1
        assert run_result_len == expected_run_result_len

        result = run_result[0]["rows"]
        expected = 0
        assert result == expected

    # TODO: Legge til flere tester

    def test_default_transient_postfix_is_added_to_table_name(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=True,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
        )
        assert sink.transient_table == "foo.bar.baz__transient"

    def test_overridden_transient_postfix_is_added_to_table_name(self):
        sink = SnowSink(
            "foo.bar.baz",
            transient=True,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
            transient_table_postfix="__override",
        )
        assert sink.transient_table == "foo.bar.baz__override"

    def test_default_transient_postfix_is_not_added_to_table_name_when_transient_is_false(
        self,
    ):
        sink = SnowSink(
            "foo.bar.baz",
            transient=False,
            connection_handler=MockSnowHandler(),
            file_handler=MockFileHandler(),
            transient_table_postfix="__override",
        )
        assert sink.table == "foo.bar.baz"
