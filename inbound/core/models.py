import datetime
from dataclasses import dataclass
from importlib.metadata import version


@dataclass
class Description:
    name: str
    type: str
    precision: int
    scale: int
    nullable: bool


@dataclass
class Metadata:
    load_time: datetime.datetime
    source_env: str
    run_id: str
    job_name: str
    inbound_version: str = version("inbound")

    def get_description(self):
        return [
            Description(
                name="_inbound__load_time",
                type="timestamp",
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
        ]
