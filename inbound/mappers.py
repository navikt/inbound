from .core.models import Description
from .sdk.mapper import Mapper


class OraToSnowDescriptionMapper(Mapper):

    def map(self, column_description: Description) -> Description:
        column = column_description
        # TODO: Lage mapping for resterende datatyper
        type_mapping = {
            "<DbType DB_TYPE_NUMBER>": "number",
            "<DbType DB_TYPE_VARCHAR>": "varchar",
            "<DbType DB_TYPE_DATE>": "datetime",  # date i oracle kan også inneholde tidspunkt
        }
        return Description(
            name=self._map_non_ascii(column.name),
            type=type_mapping.get(column.type, "varchar"),
            precision=self._map_precision(column.precision),
            scale=self._map_scale(column.scale),
            nullable=column.nullable,
        )

    def _map_non_ascii(self, name: str) -> str:
        name = name.lower()
        if name.isidentifier() and name.isascii():
            return name
        name = name.replace("æ", "ae")
        name = name.replace("ø", "o")
        name = name.replace("å", "a")
        assert name.isidentifier() and name.isascii()
        return name

    def _map_precision(self, precision):
        if precision is None:
            return 38  # Hack pga. debet / kredit felter ikke har noe informasjon om precision. Hør med Dag-Øystein
        if precision == 0:
            return 38
        return precision

    def _map_scale(self, scale):
        if scale is None:
            return 5  # Hack pga. debet / kredit felter ikke har noe informasjon om scale. Hør med Dag-Øystein
        if scale < 0:
            return 5  # TODO: Usikker på hva jeg bør gjøre her. Hør med Dag-Øystein
        return scale


class AnaplanToSnowDescriptionMapper(Mapper):

    def map(self, column_description: Description) -> Description:
        column = column_description
        type_mapping = {
            "TEXT": "varchar",
            "NUMBER": "varchar",
            "BOOLEAN": "varchar",
        }
        return Description(
            name=column.name,
            type=type_mapping[column.type],
            precision=column.precision,
            scale=column.scale,
            nullable=column.nullable,
        )


class MSSQLToSnowDescriptionMapper(Mapper):

    def map(self, column_description: Description) -> Description:
        column = column_description

        # TODO: Lage mapping for resterende datatyper
        type_mapping = {
            "<class 'bool'>": "boolean",
            "<class 'datetime.datetime'>": "datetime",
            "<class 'int'>": "number",
            "<class 'decimal.Decimal'>": "number",
            "<class 'str'>": "varchar",
            "<class 'bit'>": "boolean",
        }
        return Description(
            name=column.name,
            type=type_mapping[column.type],
            precision=column.precision,
            scale=column.scale,
            nullable=column.nullable,
        )
