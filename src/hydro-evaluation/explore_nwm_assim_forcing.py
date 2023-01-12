from rasterstats import zonal_stats
import geopandas as gpd
import xarray as xr
import rasterio
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from rasterio.features import rasterize
import numpy as np
import fiona
import pickle
import gc

import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO

# import xarray as xr
from google.cloud import storage

import grids.config as config
from grids.utils import get_cache_dir, profile
# from memory_profiler import profile
from rasterstats import zonal_stats
from rasterio.io import MemoryFile

ds = xr.open_dataset(
    "/home/matt/Downloads/nwm.20221001_forcing_analysis_assim_nwm.t00z.analysis_assim.forcing.tm00.conus.nc",
    engine='rasterio',
    # mask_and_scale=False,
    # decode_coords="all"
)
print(ds["RAINRATE"].rio.crs)