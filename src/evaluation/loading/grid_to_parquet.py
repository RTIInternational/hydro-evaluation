import gc
import os
import pickle
import config
import const
import utils
import time

import xarray as xr
import pandas as pd
import geopandas as gpd
import numpy as np

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

from pathlib import Path

from google.cloud import storage

from rasterio.io import MemoryFile


def list_blobs_forcing(
    configuration: str,
    reference_time: str,
    must_contain: str = 'channel_rt',
    bucket: str = const.NWM_BUCKET
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
    bucket = client.bucket(bucket)

    # Get list of blobs
    blobs = client.list_blobs(
        bucket,
        prefix=f'nwm.{issue_date}/{configuration}/nwm.t{issue_time}'
    )

    # Return blob names
    return [b.name for b in list(blobs) if must_contain in b.name]


def list_blobs_assim(
        configuration: str,
        issue_date: str,
        must_contain: str = 'tm00',
        bucket: str = const.NWM_BUCKET
) -> list:
    """List available blobs with provided parameters.

    Based largely on OWP HydroTools.

    Parameters
    ----------
    configuration : str, required
        No validation
    issue_date : str, required
        Issue date in 
        YYYYmmdd format.
    must_contain : str, optional, default 'tm00
        Optional substring found in each blob name.

    Returns
    -------
    blob_list : list
        A list of blob names that satisfy the criteria set by the
        parameters.
    """

    # Connect to bucket with anonymous client
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket)

    # Get list of blobs
    blobs = client.list_blobs(
        bucket,
        prefix=f'nwm.{issue_date}/{configuration}/'
    )

    # Return blob names
    return [b.name for b in list(blobs) if must_contain in b.name]


def get_blob(
    blob_name: str,
    bucket: str = const.NWM_BUCKET
) -> bytes:
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
    bucket = client.bucket(bucket)
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
    nc_filepath = os.path.join(utils.get_cache_dir(), blob_name)
    utils.make_parent_dir(nc_filepath)

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


def calculate_map_forcing(
    blob_name: str,
    weights_filepath: str,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Calculate the MAP for a single NetCDF file (i.e. one timestep).

    ToDo: add way to filter which catchments are calculated
    """
    # print(f"Processing {blob_name}, {datetime.now()}")

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
    df = calc_zonal_stats_weights(src, weights_filepath)

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
    ds.close()
    del ds
    gc.collect()

    return df


def calculate_map_assim(
    blob_name: str,
    weights_filepath: str,
    use_cache: bool = True
) -> pd.DataFrame:
    """Calculate the MAP for a single NetCDF file (i.e. one timestep).

    ToDo: add way to filter which catchments are calculated
    """
    # print(f"Processing {blob_name}")

    # Get some metainfo from blob_name
    path_split = blob_name.split("/")
    issue_datetime = datetime.strptime(
        path_split[0].split(".")[1] + path_split[2].split(".")[1],
        "%Y%m%dt%Hz"
    )
    offset_hours = int(path_split[2].split(".")[4][2:])  # tm00
    value_time = issue_datetime - timedelta(hours=offset_hours)
    configuration = path_split[1]

    # Get xr.Dataset/xr.DataArray
    ds = get_dataset(blob_name, use_cache)
    src = ds["RAINRATE"]

    # Pull out some attributes
    measurement_unit = src.attrs["units"]
    variable_name = src.attrs["standard_name"]

    # Calculate MAP
    df = calc_zonal_stats_weights(src, weights_filepath)

    # Set metainfo for MAP
    # df["reference_time"] = 0
    df["value_time"] = value_time
    df["configuration"] = configuration
    df["measurement_unit"] = measurement_unit
    df["variable_name"] = variable_name

    # Reduce memory foot print
    # df["reference_time"] = df["reference_time"].astype("category")
    df['configuration'] = df['configuration'].astype("category")
    df['measurement_unit'] = df['measurement_unit'].astype("category")
    df['variable_name'] = df['variable_name'].astype("category")
    df["catchment_id"] = df["catchment_id"].astype("category")

    # print(df.info(verbose=True, memory_usage='deep'))
    # print(df.memory_usage(index=True, deep=True))
    # print(df)

    # This should not be needed, but without memory usage grows
    ds.close()
    del ds
    gc.collect()

    return df


def main_2():
    """Calculate MAP Forcing"""

    # Setup some criteria
    ingest_days = 1
    forecast_interval_hrs = 6
    start_dt = datetime(2023, 1, 1) # First one is at 00Z in date
    td = timedelta(hours=forecast_interval_hrs)
    number_of_forecasts = 1 # int(ingest_days * 24/forecast_interval_hrs)

    # Loop though forecasts, fetch and insert
    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")

        print(f"Start download of {ref_time_str}")

        blob_list = list_blobs_forcing(
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
                calculate_map_forcing,
                blob_list,
                chunksize=chunksize
            )
        
        # Join all timesteps into single pd.DataFrame
        df = pd.concat(dfs)

        # Save as parquet file
        parquet_filepath = os.path.join(config.MEDIUM_RANGE_PARQUET, f"{ref_time_str}.parquet")
        make_parent_dir(parquet_filepath)
        df.to_parquet(parquet_filepath)

        # Print out some DataFrame stats
        print(df.info(verbose=True, memory_usage='deep'))
        print(df.memory_usage(index=True, deep=True))


def main_3():
    """Calculate MAP Assim"""

    # Setup some criteria
    start_dt = datetime(2022, 10, 1)
    number_of_days = 11

    # Loop though forecasts, fetch and insert
    for f in range(number_of_days):
        issue_date = start_dt + timedelta(days=f)
        issue_date_str = issue_date.strftime("%Y%m%d")

        print(f"Start download of {issue_date_str}")

        blob_list = list_blobs_assim(
            configuration = "forcing_analysis_assim",
            issue_date = issue_date_str,
            must_contain = "tm00.conus"
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
                calculate_map_assim,
                blob_list,
                chunksize=chunksize
            )
        
        # Join all timesteps into single pd.DataFrame
        df = pd.concat(dfs)

        # Save as parquet file
        df.to_parquet(f"map/data/forcing_analysis_assim/{issue_date_str}.parquet")

        # Print out some DataFrame stats
        print(df.info(verbose=True, memory_usage='deep'))
        print(df.memory_usage(index=True, deep=True))
        # print(df)

        del df
        gc.collect()


if __name__ == "__main__":
    # main_2()
    # main_3()
    pass