import pytest
from dbversioning.versionedDbShellUtil import (
    convert_str_none_if_empty,
    convert_str_to_bool,
    get_size_text, get_time_text)


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


def test_get_file_size():
    kb = 1024
    mb = kb ** 2
    gb = kb ** 3
    tb = kb ** 4

    assert get_size_text(0) == "0 B"
    assert get_size_text(512) == "512 B"

    assert get_size_text(kb * 1) == "1.00 KB"
    assert get_size_text(kb * 1.5) == "1.50 KB"
    assert get_size_text(2048) == "2.00 KB"

    assert get_size_text(mb) == "1.00 MB"
    assert get_size_text(mb * 1.5) == "1.50 MB"

    assert get_size_text(gb) == "1.00 GB"
    assert get_size_text(gb * 1.75) == "1.75 GB"

    assert get_size_text(tb) == "1.00 TB"
    assert get_size_text(tb * 1.75) == "1.75 TB"


def test_get_time_text():
    minute = 60.0
    hour = minute ** 2

    assert get_time_text(0.0001) == "< 0.00 sec"
    assert get_time_text(0.004) == "< 0.00 sec"
    assert get_time_text(0.005) == "0.01 sec"

    assert get_time_text(minute) == "1.00 min"
    assert get_time_text(minute * 1.5) == "1.50 min"

    assert get_time_text(hour) == "1.00 hrs"
    assert get_time_text(hour * 1.5) == "1.50 hrs"
