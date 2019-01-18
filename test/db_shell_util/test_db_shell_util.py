import pytest
from dbversioning.versionedDbShellUtil import convert_str_none_if_empty, convert_str_to_bool


def test_convert_str_none_if_empty_empty():
    assert convert_str_none_if_empty("") is None


def test_convert_str_none_if_empty_not_empty():
    assert "here" == convert_str_none_if_empty("here")


def test_convert_str_to_bool_true():
    assert convert_str_to_bool('true') is True
    assert convert_str_to_bool('t') is True


def test_convert_str_to_bool_false():
    assert convert_str_to_bool('false') is False
    assert convert_str_to_bool('f') is False


def test_convert_str_to_bool_none():
    assert convert_str_to_bool(None) is None


def test_convert_str_to_bool_error():
    with pytest.raises(ValueError) as e:
        convert_str_to_bool("None")

    assert "convert_str_to_bool: invalid values None" in str(e.value)
