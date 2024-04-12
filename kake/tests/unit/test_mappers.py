from unittest import TestCase

from kake.core.models import Description
from kake.mappers import AnaplanToSnowDescriptionMapper, OraToSnowDescriptionMapper


class TestOraMapper(TestCase):
    def test_ora_varchar(self):
        test = Description(
            name="foo",
            type="<DbType DB_TYPE_VARCHAR>",
            precision=38,
            scale=0,
            nullable=True,
        )
        result = OraToSnowDescriptionMapper().map(test)
        expected = Description(
            name="foo",
            type="varchar",
            precision=38,
            scale=0,
            nullable=True,
        )
        assert result == expected

    def test_default_to_varchar_if_dtype_mapping_not_exists(self):
        test = Description(
            name=None,
            type="<DbType DB_TYPE_CHAR>",
            precision=None,
            scale=None,
            nullable=True,
        )
        result = OraToSnowDescriptionMapper().map(test)
        expected = Description(
            name=None,
            type="varchar",
            precision=38,
            scale=5,
            nullable=True,
        )
        assert result == expected

    # TODO: Legge til flere tester


class TestAnaplanMapper(TestCase):
    def test_anaplan_text_is_always_mapped_to_varchar(self):

        test = Description(
            name=None,
            type="TEXT",
            precision=None,
            scale=None,
            nullable=True,
        )
        result = AnaplanToSnowDescriptionMapper().map(test)
        expected = Description(
            name=None,
            type="varchar",
            precision=None,
            scale=None,
            nullable=True,
        )
        assert result == expected

    def test_anaplan_number_is_always_mapped_to_varchar(self):

        test = Description(
            name=None,
            type="NUMBER",
            precision=None,
            scale=None,
            nullable=True,
        )
        result = AnaplanToSnowDescriptionMapper().map(test)
        expected = Description(
            name=None,
            type="varchar",
            precision=None,
            scale=None,
            nullable=True,
        )
        assert result == expected

    def test_anaplan_boolean_is_always_mapped_to_varchar(self):

        test = Description(
            name=None,
            type="BOOLEAN",
            precision=None,
            scale=None,
            nullable=True,
        )
        result = AnaplanToSnowDescriptionMapper().map(test)
        expected = Description(
            name=None,
            type="varchar",
            precision=None,
            scale=None,
            nullable=True,
        )
        assert result == expected
