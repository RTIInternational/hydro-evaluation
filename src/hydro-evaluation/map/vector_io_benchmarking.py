import duckdb
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from grids.utils import profile
from grids import config
import pickle


@profile
def shape_to_gdf(filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame.from_file(filepath)
    return gdf

@profile
def pgsql_to_gdf() -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame.from_postgis("SELECT * FROM public.wbdhu10_conus;", config.CONNECTION)
    return gdf

@profile
def gdf_to_pkl(gdf:gpd.GeoDataFrame, pkl_filepath: str):
    with open(pkl_filepath, 'wb') as f:
            pickle.dump(gdf, f)

@profile
def pkl_to_gdf(pkl_filepath: str) -> gpd.GeoDataFrame:
    with open(pkl_filepath, 'rb') as f:
        gdf = pickle.load(f)
    return gdf

@profile
def gdf_to_feather(gdf:gpd.GeoDataFrame, feather_filepath: str):
    gdf.to_feather(feather_filepath)


@profile
def feather_to_gdf(feather_filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_feather(feather_filepath)
    return gdf


@profile
def gdf_to_parquet(gdf:gpd.GeoDataFrame, parquet_filepath: str):
    gdf.to_parquet(parquet_filepath)


@profile
def parquet_to_gdf(parquet_filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf


shp_filepath = "/home/matt/wbdhu10_conus.shp"
pkl_filepath = "/home/matt/wbdhu10_conus.pkl"
feather_filepath = "/home/matt/wbdhu10_conus.feather"
parquet_filepath = "/home/matt/wbdhu10_conus.parquet"

gdf = shape_to_gdf(shp_filepath)

gdf_to_pkl(gdf, pkl_filepath)
pkl_gdf = pkl_to_gdf(pkl_filepath)
print(pkl_gdf.info(verbose=True, memory_usage='deep'))

gdf_to_feather(gdf, feather_filepath)
feather_gdf = feather_to_gdf(feather_filepath)
print(feather_gdf.info(verbose=True, memory_usage='deep'))

gdf_to_parquet(gdf, parquet_filepath)
parquet_gdf = parquet_to_gdf(parquet_filepath)
print(parquet_gdf.info(verbose=True, memory_usage='deep'))