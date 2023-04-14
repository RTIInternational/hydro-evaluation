from datetime import datetime

import pandas as pd
import pytest
from models import Filter
from pydantic import ValidationError
from queries import queries


def test_multiple_filters():
    filter_1 = Filter(
        column="secondary_location_id", 
        operator="in", 
        value=["123456", "9876543"]
    )
    filter_2 = Filter(
        column="reference_time", 
        operator="=", 
        value=datetime(2023, 1, 1, 0, 0, 0)
    )
    filter_str = queries.format_filters([filter_1, filter_2])
    assert filter_str == "WHERE secondary_location_id in ('123456','9876543') AND reference_time = '2023-01-01 00:00:00'"


def test_no_filters():
    filter_str = queries.format_filters([])
    assert filter_str == "--no where clause"


if __name__ == "__main__":
    # test_multiple_filters()
    # test_no_filters()
    pass