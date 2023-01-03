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


def format_filters(filters: List[dict]) -> List[str]:
    """Generate strings from filter dict.
    
   ToDo:  This is probably not robust enough.
    """
    filter_strs = []
    for f in filters:
        if type(f["value"]) == str:
            filter_strs.append(f"""{f["column"]} {f["operator"]} '{f["value"]}'""")
        else:
            filter_strs.append(f"""{f["column"]} {f["operator"]} {f["value"]}""")
    return filter_strs


def generate_map_query(group_by: List[str], order_by: List[str], filters: List[dict]) -> str:
    """Generate query string for getting MAP.

    ToDo: This needs to check for valid column names.
    
    Parameters
    ----------
    group_by: List[str]
        List of field names to select and group by.  
            This will likely always include the unique polygon name (e.g. huc10)
    order_by: List[str]
        List of fields to order results by.
    filters: List[dict]
        List of dictionaries containing `column`, `operator` and `value` that will be used 
            in the where clause of the query to filter results.  Columns referenced in filters 
            must be in `group_by`.

    Returns
    -------
    query: str
        Query to get MAP from rasters in database 
    
    Examples
    --------

    group_by=[
                "huc10",
                "reference_time", 
                "geom",
                "value_time",
                "lead_time",
            ],
    order_by=["reference_time", "huc10"],
    filters=[
        {
            "column": "huc10",
            "operator": "like",
            "value": "0303%"
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "2022-10-01 00:00:00"
        },
        {
            "column": "lead_time",
            "operator": "=",
            "value": 1
        },
        {
            "column": "lead_time",
            "operator": "in",
            "value": "(6, 12, 18, 24)"
        }
    ]


    """

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
def get_map(group_by: List[str], order_by: List[str], filters: List[dict], return_geo: bool = True) -> gpd.GeoDataFrame:
    """Get MAP from database as a gpd.GeoDataFrame.
    
    See `generate_map_query` for parameter descriptions.
    """  

    query = generate_map_query(
        group_by=group_by,
        order_by=order_by,
        filters=filters
    )

    # Get DataFrame from database
    df = pd.read_sql(text(query), config.CONNECTION)

    if return_geo:
        # Convert to GeoDataFrame
        gs = gpd.GeoSeries.from_wkb(df["geom"])
        gdf = gpd.GeoDataFrame(df, geometry=gs)

        return gdf

    return df
    
def main():

    # for i in range(5):
    #     gdf = get_map(
    #         group_by=[
    #             "huc10",
    #             "reference_time", 
    #             "geom",
    #             # "value_time",
    #             "lead_time",
    #         ],
    #         order_by=["reference_time", "huc10"],
    #         filters=[
    #             {
    #                 "column": "huc10",
    #                 "operator": "like",
    #                 "value": "0303%"
    #             },
    #             {
    #                 "column": "reference_time",
    #                 "operator": "=",
    #                 "value": "2022-10-01 00:00:00"
    #             },
    #             {
    #                 "column": "lead_time",
    #                 "operator": "=",
    #                 "value": str(i + 1)
    #             }
    #         ])
    #     gdf.plot("value", legend=True)
    #     plt.savefig(f"2022-10-01_00:00:00_t{i + 1}.png")

    
    df = get_map(
        group_by=[
            "huc10",
            "reference_time", 
            "value_time"
        ],
        order_by=["reference_time", "huc10"],
        filters=[
            {
                "column": "huc10",
                "operator": "like",
                "value": "0303%"
            },
            {
                "column": "reference_time",
                "operator": "=",
                "value": "2022-10-01 00:00:00"
            }
        ],
    return_geo=False
    )
    print(df)

    for k,v in df.groupby("huc10"):
        plt.plot(v["value_time"], v["value"])
        
    plt.savefig(f"2022-10-01_00:00:00.png")



if __name__ == "__main__":
    main()