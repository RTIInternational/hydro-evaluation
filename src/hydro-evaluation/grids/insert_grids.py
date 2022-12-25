import psycopg2
import subprocess 
import sys, os
import xarray as xr
import rioxarray
from google.cloud import storage
from io import BytesIO
import os
import subprocess
import grids.config as config
from grids.utils import profile
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor


BUCKET = "national-water-model"
GRID_DIR = os.path.join("grids", "data")


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
        blob_name : str, required
            Name of blob to retrieve.

        Returns
        -------
        ds : xarray.Dataset
            The data stored in the blob.
        
        """
        nc_filepath = os.path.join(config.NWM_CACHE, _format_nc_name(blob_name))

        # If the file exists and use_cache = True
        if os.path.exists(nc_filepath) and use_cache:

            # Get dataset from cache
            ds = xr.load_dataset(
                nc_filepath,
                engine='h5netcdf',
                mask_and_scale=False,
                decode_coords="all"
                )

            return ds
        
        else:

            # Get raw bytes
            raw_bytes = get_blob(blob_name)

            # Create Dataset
            ds = xr.load_dataset(
                BytesIO(raw_bytes),
                engine='h5netcdf',
                mask_and_scale=False,
                decode_coords="all"
                )

            if use_cache:
                # Subset and cache
                ds["RAINRATE"].to_netcdf(
                    nc_filepath,
                    engine='h5netcdf',
                )

            return ds


def ds_to_tiff(ds: xr.Dataset, variable: str, blob_name: str) -> str:
    """Saves xr.Dataset to geotiff"""

    filepath = os.path.join(config.NWM_CACHE, _format_tiff_name(blob_name))

    ds[variable].rio.to_raster(
        filepath,
        compress='LZW'
    )

    return filepath


def delete_tiff(filepath: str):
    if os.path.exists(filepath):
        os.remove(filepath)
    else:
        print(f"{filepath} does not exist")


def _format_tiff_name(blob_name: str):
    return blob_name.replace("/", ".").replace(".nc", ".tiff")


def _format_nc_name(blob_name: str):
    return blob_name.replace("/", ".")


def load_raster_to_db(filepath: str):

    # Build command string
    cmd = f'raster2pgsql -F -a -t "288x256" {filepath} forcing_medium_range | psql {config.CONNECTION}'

    # Execute
    subprocess.call(cmd, shell=True)


def get_load_raster(blob_name: str, use_cache: bool = True):
    """Get and load the raster to the DB"""
    print(f"Fetching {blob_name}")
    ds = get_dataset(blob_name)
    tiff_filepath = ds_to_tiff(ds, "RAINRATE", blob_name)
    load_raster_to_db(tiff_filepath)
    delete_tiff(tiff_filepath)


@profile
def main():

    # Setup some criteria
    ingest_days = 1
    forecast_interval_hrs = 6
    start_dt = datetime(2022, 10, 1) # First one is at 00Z in date
    td = timedelta(hours=forecast_interval_hrs)
    number_of_forecasts = int(ingest_days * 24/forecast_interval_hrs)

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

        # for blob_name in blob_list:
        #     get_load_raster(blob_name)

        # Set max processes
        max_processes = max((os.cpu_count() - 2), 1)
        chunksize = (len(blob_list) // max_processes) + 1

        # Retrieve data using multiple processes
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            executor.map(
                get_load_raster, 
                blob_list,
                chunksize=chunksize
            )


if __name__ == "__main__":
    main()