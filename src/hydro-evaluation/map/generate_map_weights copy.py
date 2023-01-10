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

import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO

# import xarray as xr
from google.cloud import storage

import grids.config as config
from grids.utils import get_cache_dir, profile
from rasterstats import zonal_stats


BUCKET = "national-water-model"


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

        # If the file exists and use_cache = True
        if os.path.exists(nc_filepath) and use_cache:

            # Get dataset from cache
            ds = xr.load_dataset(
                nc_filepath,
                engine="rasterio",
                # engine='h5netcdf',
                # mask_and_scale=False,
                # decode_coords="all"
            )

            return ds
        
        else:

            # Get raw bytes
            raw_bytes = get_blob(blob_name)

            # Create Dataset
            ds = xr.load_dataset(
                BytesIO(raw_bytes),
                engine="rasterio",
                # engine='h5netcdf',
                # mask_and_scale=False,
                # decode_coords="all"
                )

            if use_cache:
                # Subset and cache
                ds["RAINRATE"].to_netcdf(
                    nc_filepath,
                    engine="rasterio",
                    # decode_coords="all",
                    # engine='h5netcdf',
                )

            return ds


def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    ds: xr.Dataset
    ) -> str:

    gdf_proj = gdf.to_crs(config.CONUS_NWM_WKT)

    src = ds["RAINRATE"]

    crosswalk_dict = {}

    for index, row in gdf_proj.iterrows():
        geom_rasterize = rasterize([(row["geometry"], 1)],
                                out_shape=src.rio.shape,
                                transform=src.rio.transform(),
                                all_touched=True,
                                fill=0,
                                dtype='uint8')

        crosswalk_dict[index] = np.where(geom_rasterize == 1)

    with open('saved_weights2.pkl', 'wb') as f:
        pickle.dump(crosswalk_dict, f)


def calc_zonal_stats_weights(
    ds: xr.Dataset,
):

    src = ds["RAINRATE"]

    with open('saved_weights2.pkl', 'rb') as f:
        crosswalk_dict = pickle.load(f)

    r_array = src.values[0]
    r_array[r_array == src.rio.nodata] = np.nan

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(r_array[value])

    df = pd.DataFrame.from_dict(mean_dict,
        orient='index',
        columns=['mean'])

    return df


def add_zonalstats_to_gdf_weights(
    gdf: gpd.GeoDataFrame,
    ds: xr.Dataset,
) -> gpd.GeoDataFrame:
    """Calculates zonal stats and adds to GeoDataFrame"""

    src = ds["RAINRATE"]

    with open('saved_weights2.pkl', 'rb') as f:
        crosswalk_dict = pickle.load(f)

    r_array = src.values[0]
    r_array[r_array == src.rio.nodata] = np.nan

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(r_array[value])

    df = pd.DataFrame.from_dict(mean_dict,
        orient='index',
        columns=['mean'])

    # adding statistics back to original GeoDataFrame
    gdf2 = pd.concat([gdf, df], axis=1)
    return gdf2



def make_gdf_canonical(gdf: gpd.GeoDataFrame):
    pass

def _format_nc_name(blob_name: str):
    return blob_name.replace("/", ".")


def calculate_map(blob_name: str, use_cache: bool = True):
    """Get and load the raster to the DB"""
    print(f"Fetching {blob_name}")

    # nwm.20221001/forcing_medium_range/nwm.t00z.medium_range.forcing.f001.conus.nc
    path_split = blob_name.split("/")
    ref_date_str = path_split[0].split(".")[1] # 20221001
    ref_time_str = path_split[2].split(".")[1] # t00z
    reference_time = datetime.strptime(ref_date_str + ref_time_str, "%Y%m%dt%Hz")
    offset_hours = int(path_split[2].split(".")[4][1:]) # f001
    value_time = reference_time + timedelta(hours=offset_hours)
    configuration = path_split[1]

    # huc10_gdf = shape_to_gdf('/home/matt/wbdhu10_conus.shp')
    # huc10_gdf = shape_to_gdf('/home/matt/huc10_lcc.shp')
    # print(huc10_gdf)

    ds = get_dataset(blob_name, use_cache)

    measurement_units = ds["RAINRATE"].attrs["units"]
    variable_name = ds["RAINRATE"].attrs["standard_name"]

    # generate_weights_file(huc10_gdf, ds)

    df = calc_zonal_stats_weights(ds)
    print(df)

    # gdf = add_zonalstats_to_gdf_weights(huc10_gdf, ds)

    # Do something with the data
    # gdf.plot("mean", legend=True)
    # plt.savefig(f"map_weights_test.png")


@profile
def main():

    # Setup some criteria
    ingest_days = 1
    forecast_interval_hrs = 6
    start_dt = datetime(2022, 10, 1) # First one is at 00Z in date
    td = timedelta(hours=forecast_interval_hrs)
    number_of_forecasts = 1  # int(ingest_days * 24/forecast_interval_hrs)

    # Loop though forecasts, fetch and insert
    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")
        
        print(f"Start download of {ref_time_str}")

        blob_list = list_blobs(
            configuration = "forcing_medium_range",
            reference_time = ref_time_str,
            must_contain = "forcing"
        )[:1]

        for blob_name in blob_list:
            # ds = get_dataset(blob_name, use_cache=True)
            calculate_map(blob_name)

        # # Set max processes
        # max_processes = max((os.cpu_count() - 2), 1)
        # chunksize = (len(blob_list) // max_processes) + 1

        # # Retrieve data using multiple processes
        # with ProcessPoolExecutor(max_workers=max_processes) as executor:
        #     executor.map(
        #         get_load_raster, 
        #         blob_list,
        #         chunksize=chunksize
        #     )

    # huc10 = gpd.read_file("/home/matt/huc10_lcc.shp")

    # fc_nc = "/home/matt/cache/nwm.20221001.forcing_medium_range.nwm.t00z.medium_range.forcing.f001.conus.nc"

    # reng = "rasterio"


    # # fc_xds = xr.open_dataset(fc_nc, engine=reng)
    # # fc_xds["RAINRATE"].rio.to_raster("temp.tif")
    # # with rasterio.open("temp.tif") as src:
    # #     affine = src.transform
    # #     array = src.read(1)
    # #     df_zonal_stats = pd.DataFrame(zonal_stats(huc10, array, affine=affine))

    # # # adding statistics back to original GeoDataFrame
    # # gdf2 = pd.concat([huc10, df_zonal_stats], axis=1) 
    # # print(gdf2)

    # with xr.open_dataset(fc_nc, engine=reng) as _xds:
    #     _src = _xds.RAINRATE
    #     _aff2 = _src.rio.transform()
    #     _arr2 = _src.values[0]
    #     _df_zonal_stats = pd.DataFrame(zonal_stats(huc10, _arr2, affine=_aff2, nodata=-999))
        
    # # adding statistics back to original GeoDataFrame
    # gdf3 = pd.concat([huc10, _df_zonal_stats], axis=1)
    # print(gdf3)


if __name__ == "__main__":
    main()