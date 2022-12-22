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
def get_map():
    """Get map from database
    """  

    qry = """
        WITH medium_range_map AS (
            SELECT
            huc10.huc10 AS huc10,
            raster.reference_time,
            raster.reference_time + INTERVAL '1 hour' * raster.lead_time AS value_time,
            raster.lead_time,
            (ST_SummaryStatsAgg(ST_Clip(raster.rast, huc10.geom, true), 1, true)).mean
            FROM
                forcing_medium_range_attrs as raster
            INNER join huc10 on
                ST_INTERSECTS(huc10.geom, raster.rast)
            GROUP BY
                huc10, reference_time, lead_time
            ORDER BY
                reference_time, lead_time
        )
        SELECT * FROM medium_range_map WHERE huc10 = '0505000103';
    """

    df = pd.read_sql(qry, config.CONNECTION)
    print(df.info(memory_usage="deep"))
    print(df)

    # df = pd.read_sql(query_3, config.CONNECTION)
    # print(df.info(memory_usage="deep"))
    # print(df)
    # print(df[["reference_time","nwm_feature_id", "value_time", "forecast_value", "observed_value"]])

    # sdf = df.loc[df["reference_time"] == "2022-10-01 00:00:00"] 
    # df.plot.scatter(legend=False, x="bias", y="max_forecast_delta")
    # plt.savefig("test.png")



if __name__ == "__main__":
    get_map()