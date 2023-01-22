import os

import geopandas as gpd
import pandas as pd
import psycopg2
import psycopg2.extras

import wide_table.config as config
import wide_table.utils as utils
from wide_table.utils import profile


@profile
def insert_route_links(df: pd.DataFrame):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude)
    )
    utils.insert_bulk(
        gdf[[
            "nwm_feature_id",
            "to_node",
            "from_node",
            "gage_id",
            "geometry"
        ]],
        "route_links",
        [
            "nwm_feature_id",
            "to_node",
            "from_node",
            "gage_id",
            "geom"
        ]
    )


if __name__ == "__main__":
    route_link_data = utils.get_route_links()
    # print(route_link_data)
    insert_route_links(route_link_data)
