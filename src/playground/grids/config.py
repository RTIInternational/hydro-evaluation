import os

# import tempfile

CONNECTION = "postgresql://postgres:postgrespassword@localhost:5432/postgres"
# TEMP_DIR = tempfile.gettempdir()
CACHE_DIR = os.path.join("/home", "matt", "cache")
NWM_CACHE_DIR = os.path.join(CACHE_DIR, "nwm")
GEO_CACHE_DIR = os.path.join(CACHE_DIR, "geo")
PARQUET_CACHE_DIR = os.path.join(CACHE_DIR, "parquet")
MEDIUM_RANGE_PARQUET = os.path.join(PARQUET_CACHE_DIR, "forcing_medium_range")
SHP_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.shp")
PARQUET_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.parquet")
MEDIUM_RANGE_WEIGHTS_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_medium_range_weights.pkl")

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]'

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]'