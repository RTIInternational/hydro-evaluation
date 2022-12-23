import io
import time
from io import StringIO
from typing import List
from functools import wraps
from pathlib import Path
import json
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
import psycopg2.extras
import wide_table.config as config
import wide_table.queries as queries
from wide_table.utils import profile
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


def format_filters(filters):
        return [f'{f["column"]} {f["operator"]} {f["value"]}' for f in filters]


def generate_map_query(group_by: List[str], order_by: List[str], filters: List[dict]) -> str:
    query = f"""
        WITH medium_range_map AS (
            SELECT
                huc10.huc10 AS huc10,
                huc10.geom as geom,
                raster.reference_time,
                --raster.reference_time + INTERVAL '1 hour' * raster.lead_time AS value_time,
                --raster.lead_time,
                (ST_SummaryStatsAgg(ST_Clip(raster.rast, huc10.geom, true), 1, true)).*
            FROM
                forcing_medium_range_attrs as raster
            INNER join huc10 on
                ST_INTERSECTS(huc10.geom, raster.rast)
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
    """Get map from database
    """  

    query = generate_map_query(
        group_by=["huc10", "reference_time", "geom"],
        order_by=["reference_time"],
        filters=[
            {
                "column": "huc10",
                "operator": "like",
                "value": "'030501%'"
            },
            {
                "column": "reference_time",
                "operator": "=",
                "value": "'2022-10-01 12:00:00'"
            }
        ]
    )
 
    # print(query)
    df = pd.read_sql(text(query), config.CONNECTION)
    print(df.info(memory_usage="deep"))
    gs = gpd.GeoSeries.from_wkb(df["geom"])
    gdf = gpd.GeoDataFrame(df, geometry=gs)
    gdf.plot("mean", legend=True)
    plt.savefig("2022-10-01_12:00:00.png")
    

    # sdf = df.loc[df["reference_time"] == "2022-10-01 00:00:00"] 
    # df.plot.scatter(legend=False, x="bias", y="max_forecast_delta")
    # plt.savefig("test.png")



if __name__ == "__main__":
    get_map()