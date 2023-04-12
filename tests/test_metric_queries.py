from datetime import datetime

import pandas as pd
# import pytest
from models import MetricFilter
from pydantic import ValidationError
from queries import queries


def test_query():
            
    filter_str = queries.calculate_nwm_feature_metrics(
        forecast_dir="path/to/forecast/parquet",
        observed_dir="path/to/observed/parquet",
        group_by=["nwm_feature_id"],
        order_by=["nwm_feature_id"],
        filters=[{
            "column": "reference_time",
            "operator": "=",
            "value": "2022-01-01T00:00:00"
        }]
    )
    print(filter_str)



if __name__ == "__main__":
    test_query()
    pass