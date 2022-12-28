import io
import json
import time
from functools import wraps
from io import StringIO
from pathlib import Path
from typing import List

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text

import grids.config as config
from grids.utils import profile


def format_filters(filters):
        return [f"""{f["column"]} {f["operator"]} '{f["value"]}'""" for f in filters]


def generate_map_query(group_by: List[str], order_by: List[str], filters: List[dict]) -> str:
    """Generate query string for getting MAP"""

    query = f"""
        WITH medium_range_map AS (
            SELECT
                {",".join(group_by)},
                (ST_SummaryStatsAgg(ST_Clip(rast, geom, true), 1, true)).mean as value
            FROM
                forcing_medium_range_attrs as raster
            INNER JOIN huc10 on
                ST_INTERSECTS(geom, rast)
            GROUP BY
                {",".join(group_by)}
            ORDER BY
                {",".join(order_by)}
        )
        SELECT * FROM medium_range_map WHERE {" AND ".join(format_filters(filters))};
    """
    return query


@profile
def get_map():
    """Get MAP from database."""  

    query = generate_map_query(
        group_by=[
            "huc10",
            "reference_time", 
            "geom",
            # "value_time",
            # "lead_time",
        ],
        order_by=["reference_time"],
        filters=[
            {
                "column": "huc10",
                "operator": "like",
                "value": "03%"
            },
            {
                "column": "reference_time",
                "operator": "=",
                "value": "2022-10-01 00:00:00"
            }
        ]
    )
 
    # print(query)
    df = pd.read_sql(text(query), config.CONNECTION)
    print(df.info(memory_usage="deep"))
    print(df)

    gs = gpd.GeoSeries.from_wkb(df["geom"])
    gdf = gpd.GeoDataFrame(df, geometry=gs)
    gdf.plot("value", legend=True)
    plt.savefig("2022-10-01_00:00:00.png")
    

if __name__ == "__main__":
    get_map()