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


BUCKET = "national-water-model"
WEIGHTS_FILE_NAME = "wbdhu10_forcing_medium_range.pkl"


def _format_nc_name(blob_name: str):
    return blob_name.replace("/", ".")


def shape_to_gdf(filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame.from_file(filepath)
    return gdf


def list_blobs(
        configuration: str,
        reference_time: str,
        must_contain: str = 'channel_rt'
) -> list:
    """List available blobs with provided parameters.

    Based largely on OWP HydroTools.

    Parameters
    ----------
    configuration : str, required
        No validation
    reference_time : str, required
        Model simulation or forecast issuance/reference time in 
        YYYYmmddTHHZ format.
    must_contain : str, optional, default 'channel_rt'
        Optional substring found in each blob name.

    Returns
    -------
    blob_list : list
        A list of blob names that satisfy the criteria set by the
        parameters.
    """

    # Break-up reference time
    tokens = reference_time.split('T')
    issue_date = tokens[0]
    issue_time = tokens[1].lower()

    # Connect to bucket with anonymous client
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(BUCKET)

    # Get list of blobs
    blobs = client.list_blobs(
        bucket,
        prefix=f'nwm.{issue_date}/{configuration}/nwm.t{issue_time}'
    )

    # Return blob names
    return [b.name for b in list(blobs) if must_contain in b.name]


def get_blob(blob_name: str) -> bytes:
    """Retrieve a blob from the data service as bytes.

    Based largely on OWP HydroTools.

    Parameters
    ----------
    blob_name : str, required
        Name of blob to retrieve.

    Returns
    -------
    data : bytes
        The data stored in the blob.

    """
    # Setup anonymous client and retrieve blob data
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(BUCKET)
    return bucket.blob(blob_name).download_as_bytes(timeout=120)


def get_dataset(
        blob_name: str,
        use_cache: bool = True
) -> xr.Dataset:
    """Retrieve a blob from the data service as xarray.Dataset.

    Based largely on OWP HydroTools.

    Parameters
    ----------
    blob_name: str, required
        Name of blob to retrieve.
    use_cacahe: bool, default True
        If cache should be used.  
        If True, checks to see if file is in cache, and 
        if fetched from remote will save to cache.

    Returns
    -------
    ds : xarray.Dataset
        The data stored in the blob.

    """
    nc_filepath = os.path.join(
        get_cache_dir(),
        _format_nc_name(blob_name)
    )
    # print(f"file path: {nc_filepath}")

    # If the file exists and use_cache = True
    if os.path.exists(nc_filepath) and use_cache:

        # Get dataset from cache
        ds = xr.load_dataset(
            nc_filepath,
            engine='h5netcdf',
        )

        return ds

    else:

        # Get raw bytes
        raw_bytes = get_blob(blob_name)

        # Create Dataset
        ds = xr.load_dataset(
            MemoryFile(raw_bytes),
            engine='h5netcdf',
        )

        if use_cache:
            # Subset and cache
            ds["RAINRATE"].to_netcdf(
                nc_filepath,
                engine='h5netcdf',
            )

        return ds


def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
    crosswalk_dict_key: str = None
):
    """Generate a weights file."""

    gdf_proj = gdf.to_crs(config.CONUS_NWM_WKT)

    crosswalk_dict = {}
    # This is a probably a really poor performing way to do this
    for index, row in gdf_proj.iterrows():
        geom_rasterize = rasterize([(row["geometry"], 1)],
                                   out_shape=src.rio.shape,
                                   transform=src.rio.transform(),
                                   all_touched=True,
                                   fill=0,
                                   dtype='uint8')
        if crosswalk_dict_key:
            crosswalk_dict[row[crosswalk_dict_key]
                           ] = np.where(geom_rasterize == 1)
        else:
            crosswalk_dict[index] = np.where(geom_rasterize == 1)

    with open(weights_filepath, 'wb') as f:
        pickle.dump(crosswalk_dict, f)


def calc_zonal_stats_weights(
    src: xr.DataArray,
    weights_filepath: str,
) -> pd.DataFrame:
    """Calculates zonal stats"""

    # Open weights dict from pickle
    # This could probably be done once and passed as a reference.
    with open(weights_filepath, 'rb') as f:
        crosswalk_dict = pickle.load(f)

    r_array = src.values[0]
    r_array[r_array == src.rio.nodata] = np.nan

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(r_array[value])

    df = pd.DataFrame.from_dict(mean_dict,
                                orient='index',
                                columns=['value'])

    df.reset_index(inplace=True, names="catchment_id")
    
    # This should not be needed, but without memory usage grows
    del crosswalk_dict
    del f
    gc.collect()

    return df


def add_zonalstats_to_gdf_weights(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
) -> gpd.GeoDataFrame:
    """Calculates zonal stats and adds to GeoDataFrame"""

    df = calc_zonal_stats_weights(src, weights_filepath)
    gdf_map = gdf.merge(df, left_on="huc10", right_on="catchment_id")

    return gdf_map


# @profile
def calculate_map(
    blob_name: str,
    use_cache: bool = True
) -> pd.DataFrame:
    """Calculate the MAP for a single NetCDF file (i.e. one timestep).
    
    ToDo: add way to filter which catchments are calculated
    """
    print(f"Processing {blob_name}")

    # Get some metainfo from blob_name
    path_split = blob_name.split("/")
    reference_time = datetime.strptime(
        path_split[0].split(".")[1] + path_split[2].split(".")[1], 
        "%Y%m%dt%Hz"
    )
    offset_hours = int(path_split[2].split(".")[4][1:])  # f001
    value_time = reference_time + timedelta(hours=offset_hours)
    configuration = path_split[1]

    # Get xr.Dataset/xr.DataArray
    ds = get_dataset(blob_name, use_cache)
    src = ds["RAINRATE"]

    # Pull out some attributes
    measurement_unit = src.attrs["units"]
    variable_name = src.attrs["standard_name"]

    # Calculate MAP
    df = calc_zonal_stats_weights(src, WEIGHTS_FILE_NAME)
    
    # Set metainfo for MAP
    df["reference_time"] = reference_time
    df["value_time"] = value_time
    df["configuration"] = configuration
    df["measurement_unit"] = measurement_unit
    df["variable_name"] = variable_name

    # Reduce memory foot print
    df['configuration'] = df['configuration'].astype("category")
    df['measurement_unit'] = df['measurement_unit'].astype("category")
    df['variable_name'] = df['variable_name'].astype("category")
    df["catchment_id"] = df["catchment_id"].astype("category")

    # print(df.info(verbose=True, memory_usage='deep'))
    # print(df.memory_usage(index=True, deep=True))
    # print(df)

    # This should not be needed, but without memory usage grows
    del ds
    gc.collect()

    return df

def main_1():
    """Geenerate the weights file."""
    # Not 100% sure how best to manage this yet.  Hope a pattern will emerge.
    shp_filepath = "/home/matt/wbdhu10_conus.shp"
    huc10_gdf = shape_to_gdf(shp_filepath)
    template_blob_name = "nwm.20221001/forcing_medium_range/nwm.t00z.medium_range.forcing.f001.conus.nc"
    ds = get_dataset(template_blob_name, use_cache=True)
    src = ds["RAINRATE"]
    generate_weights_file(huc10_gdf, src, WEIGHTS_FILE_NAME, crosswalk_dict_key="huc10")


@profile
def main_2():
    """Calculate MAP"""

    # Setup some criteria
    ingest_days = 1
    forecast_interval_hrs = 6
    start_dt = datetime(2022, 10, 1) # First one is at 00Z in date
    td = timedelta(hours=forecast_interval_hrs)
    number_of_forecasts = 2  # int(ingest_days * 24/forecast_interval_hrs)

    # Loop though forecasts, fetch and insert
    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")

        print(f"Start download of {ref_time_str}")

        blob_list = list_blobs(
            configuration = "forcing_medium_range",
            reference_time = ref_time_str,
            must_contain = "forcing"
        )

        # This can be used to run serial
        # dfs = []
        # for blob_name in blob_list:
        #     df = calculate_map(blob_name, use_cache=True)
        #     dfs.append(df)

        # Set max processes
        max_processes = max((os.cpu_count() - 2), 1)
        chunksize = (len(blob_list) // max_processes) + 1

        # Retrieve data using multiple processes
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            dfs = executor.map(
                calculate_map,
                blob_list,
                chunksize=chunksize
            )
        
        # Join all timesteps into single pd.DataFrame
        df = pd.concat(dfs)

        # Save as parquet file
        df.to_parquet(f"map/data/{ref_time_str}.parquet")

        # Print out some DataFrame stats
        print(df.info(verbose=True, memory_usage='deep'))
        print(df.memory_usage(index=True, deep=True))
        # print(df)


if __name__ == "__main__":
    # main_1()
    main_2()
