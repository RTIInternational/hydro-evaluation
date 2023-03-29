import gc
import os
import pickle
import json

import config
import const
import time

from pathlib import Path
import geopandas as gpd
from functools import wraps

from datetime import datetime, timedelta

import geopandas as gpd
import spatialpandas as sp
from spatialpandas.io import read_parquet

import numpy as np
import pandas as pd

import xarray as xr

from google.cloud import storage

from rasterio.io import MemoryFile


def get_cache_dir(create: bool = True):
    if not os.path.exists(config.NWM_CACHE_DIR) and create:
        os.mkdir(config.NWM_CACHE_DIR)
    if not os.path.exists(config.NWM_CACHE_DIR):
        raise NotADirectoryError
    return config.NWM_CACHE_DIR


def make_parent_dir(filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        return retval

    return inner


def shape_to_gdf(filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame.from_file(filepath)
    return gdf


def gdf_to_parquet(gdf: gpd.GeoDataFrame, parquet_filepath: str):
    gdf.to_parquet(parquet_filepath)


def parquet_to_gdf(parquet_filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf


def parquet_to_sdf(parquet_filepath: str) -> sp.GeoDataFrame:
    sdf = read_parquet(parquet_filepath)
    return sdf


def get_usgs_gages():
    gdf = parquet_to_gdf(config.ROUTE_LINK_PARQUET)
    return gdf.loc[gdf["gage_id"].str.strip() != ""].loc[~gdf["gage_id"].str.contains("[a-zA-Z]").fillna(False)]

def np_to_list(t):
    return [a.tolist() for a in t]


def list_to_np(l):
    return tuple([np.array(a) for a in l])


def save_weights_dict(weights: dict, filepath: str):
    # To json
    j = json.dumps({k: np_to_list(v) for k, v in crosswalk_dict.items()})
    
    # Write to disk
    with open(filepath, "w") as f:
        f.write(j)

def read_weights_file(filepath: str) -> dict:
    with open(filepath, "r") as f:
        j = f.read()
    
    # Back to dict
    a = {k: list_to_np(v) for k, v in json.loads(j).items()}
    return a
