import pytest
import pandas as pd
from datetime import datetime
from pydantic import ValidationError
from models import MetricFilter
from queries import queries


def test_mutiple_filters():
    filter_1 = MetricFilter(
        column="nwm_feature_id", 
        operator="in", 
        value=["123456", "9876543"]
    )
    filter_2 = MetricFilter(
        column="reference_time", 
        operator="=", 
        value=datetime(2023, 1, 1, 0, 0, 0)
    )
    filter_str = queries.format_filters([filter_1, filter_2])
    assert filter_str == "WHERE nwm_feature_id in ('123456','9876543') AND reference_time = '2023-01-01 00:00:00'"


def test_no_filters():
    filter_str = queries.format_filters([])
    assert filter_str == "--no where clause"


if __name__ == "__main__":
    # test_filter_string()
    # test_filter_int()
    # test_filter_float()
    # test_filter_datetime()
    # test_in_filter_string_wrong_operator()
    # test_in_filter_string_wrong_value_type()
    # test_in_filter_string()
    # test_in_filter_int()
    # test_in_filter_float()
    # test_in_filter_datetime()
    pass