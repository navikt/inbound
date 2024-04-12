from unittest import TestCase

from kake.sdk.utils import get_query_list


class TestGetQueryList(TestCase):
    def test_highwatermark(self):
        query_template = "{{ highwatermark['a'] }}"
        highwatermarks = [{"a": "1"}]
        result = get_query_list(
            query_template=query_template, highwatermarks=highwatermarks
        )
        expected = ["1"]
        assert result == expected

    def test_multiple_highwatermarks(self):
        query_template = "{{ highwatermark['a'] }}"
        highwatermarks = [
            {"a": "1"},
            {"a": "2"},
        ]
        result = get_query_list(
            query_template=query_template, highwatermarks=highwatermarks
        )
        expected = ["1", "2"]
        assert result == expected

    def test_empty_list_of_highwatermarks(self):
        query_template = "foo {{ highwatermark.get('A', 'null') }}"
        highwatermarks = []
        result = get_query_list(
            query_template=query_template, highwatermarks=highwatermarks
        )
        print(result)
        expected = []
        assert result == expected

    def test_get_query_list(self):
        query_template = "select 1"
        result = get_query_list(query_template=query_template)
        expected = ["select 1"]
        assert result == expected
