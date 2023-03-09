import os
from pathlib import Path


CACHE_DIR = Path("/home", "jovyan", "shared", "rti-eval")
NWM_CACHE_DIR = os.path.join(CACHE_DIR, "nwm")
USGS_CACHE_DIR = os.path.join(CACHE_DIR, "usgs")
GEO_CACHE_DIR = os.path.join(CACHE_DIR, "geo")

NWM_CACHE_H5 = os.path.join(NWM_CACHE_DIR, "gcp_client.h5")

PARQUET_CACHE_DIR = os.path.join(CACHE_DIR, "parquet")
MEDIUM_RANGE_FORCING_PARQUET = os.path.join(PARQUET_CACHE_DIR, "forcing_medium_range")
FORCING_ANALYSIS_ASSIM_PARQUET = os.path.join(PARQUET_CACHE_DIR, "forcing_analysis_assim")
MEDIUM_RANGE_1_PARQUET = os.path.join(PARQUET_CACHE_DIR, "medium_range_mem1")
USGS_PARQUET = os.path.join(PARQUET_CACHE_DIR, "usgs")

HUC10_SHP_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.shp")
HUC10_PARQUET_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.parquet")
HUC10_MEDIUM_RANGE_WEIGHTS_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_medium_range_weights.pkl")

ROUTE_LINK_FILE = os.path.join(NWM_CACHE_DIR, "RouteLink_CONUS.nc")
ROUTE_LINK_PARQUET = os.path.join(NWM_CACHE_DIR, "route_link_conus.parquet")

