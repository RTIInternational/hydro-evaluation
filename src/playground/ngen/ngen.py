"""
Tools to connect to and cache the NextGen model outputs from "NextGen-in-a-Box"

We will try a few different approaches.
"""

import geopandas as gpd

def get_catchment_gdf(filepath):
    return gpd.read_file(filepath)
