import io
import time
from io import StringIO
from typing import List
from functools import wraps
from pathlib import Path
import json
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import wide_table.config as config
import wide_table.queries as queries
from wide_table.utils import profile
import matplotlib.pyplot as plt

@profile
def get_metrics():
    """Get metrics
    """  

    query_1 = queries.calculate_metrics(
        group_by=["reference_time", "nwm_feature_id"],
        order_by=["nwm_feature_id", "max_forecast_delta"],
        filters=[
            {
                "column": "nwm_feature_id",
                "operator": "in",
                "value": "(6731199,2441678,14586327,8573705,2567762,41002752,8268521,41026212,4709060,20957306)"
            }
        ]
    )

    query_2 = queries.calculate_metrics(
        group_by=["reference_time", "nwm_feature_id", "value_time",],
        order_by=["nwm_feature_id", "value_time"],
        filters=[
            {
                "column": "nwm_feature_id",
                "operator": "=",
                "value": "17003262"
            }
        ]
    )

    query_3 = queries.get_raw_timeseries(
        filters=[
            {
                "column": "nwm_feature_id",
                "operator": "=",
                "value": "17003262"
            }
        ]
    )

    df = pd.read_sql(query_3, config.CONNECTION)
    print(df.info(memory_usage="deep"))
    print(df.head())
    df.head().to_csv("query_3_head.csv")
    # print(df[["reference_time","nwm_feature_id", "bias", "max_forecast_delta"]])

    # df = pd.read_sql(query_3, config.CONNECTION)
    # print(df.info(memory_usage="deep"))
    # print(df)
    # print(df[["reference_time","nwm_feature_id", "value_time", "forecast_value", "observed_value"]])

    # sdf = df.loc[df["reference_time"] == "2022-10-01 00:00:00"] 
    # df.plot.scatter(legend=False, x="bias", y="max_forecast_delta")
    # plt.savefig("test.png")

    # for k,v in df.groupby("reference_time"):
    #     plt.plot(v["value_time"], v["bias"])
        
    # plt.savefig(f"test.png")

    # with psycopg2.connect(config.CONNECTION) as conn:
    #     with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
    #         cursor.execute(query_1, ())
    #         results = cursor.fetchall()
    #         print(len(results))
    #         for row in results:
    #             print(json.dumps(row, indent=4, sort_keys=True, default=str))
    #         conn.commit()
    #     cursor.close()
    # conn.close()


if __name__ == "__main__":
    get_metrics()