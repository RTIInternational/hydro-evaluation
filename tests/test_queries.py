import sys

sys.path.append("src/evaluation")
import pandas as pd
from models import MetricFilter, MetricQuery
from queries import queries

# d = pd.read_csv("tests/data/test_short_obs.csv")
# d.to_parquet("tests/data/test_short_obs.parquet")

def test_query_1():
    qry_filter = MetricQuery(column=, operator, value)
    filters = [
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "123456"
        }
    ]
    qry = queries.format_filters(filters)
    print(qry)


# query_str = queries.calculate_nwm_feature_metrics(
#     "tests/data/test_short_fcast.parquet",
#     "tests/data/test_short_obs.parquet",
#     ["id"], 
#     ["reference_time"],
#     {}
# )
# print(query_str)

if __name__ == "__main__":
    test_query_1()