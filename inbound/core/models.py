import datetime
from dataclasses import dataclass


@dataclass
class Description:
    name: str
    type: str
    precision: int
    scale: int
    nullable: bool


@dataclass
class Metadata:
    lastet_tidspunkt: datetime.datetime
    kildemiljo: str
    jobb_id: str
    jobb_navn: str
    versjon: str

    def get_description(self):
        return [
            Description(
                name="_inbound__lastet_tidspunkt",
                type="timestamp",
                precision=None,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__kildemiljo",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__jobb_id",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__jobb_navn",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
            Description(
                name="_inbound__versjon",
                type="varchar",
                precision=200,
                scale=None,
                nullable=False,
            ),
        ]
