import os
import time

import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
import xarray as xr
import dask
import pandas as pd
import numpy as np


t0 = time.time()


def gen_json(u: str, fs: fsspec.filesystem, json_dir: str) -> str:
    """
    Helper function for creating single-file kerchunk reference jsons

    Parameters
    ----------
    u : str
        Path to individual NWM file on GCS
    fs : fsspec.filesystem
        fsspec filesystem object for discovering GCS files
    json_dir : str
        Directory for the kerchunk reference files

    Returns
    -------
    str
        Path to the single-file kerchunk reference json
    """
    so = dict(
        mode="rb", anon=True, default_fill_cache=False, default_cache_type="first"
    )  # args to fs.open()
    # default_fill_cache=False avoids caching data in between file chunks to lowers memory usage.

    with fs.open(u, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        p = u.split("/")
        date = p[3]
        fname = p[5]
        outf = f"{json_dir}{date}.{fname}.json"
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
    return outf


def build_single_jsons(
    gcs_dir: str,
    component: str,
    start_date: str,
    ingest_days: int,
    configuration: str,
    json_dir: str,
) -> list:
    """
    Writes the kerchunk json reference files for each nwm file

    Parameters
    ----------
    gcs_dir : str
        GCS directory of the NWM data
    component : str
        Component of the NWM forecast category
    start_date : str
        Beginning date defining data ingestion time period
    ingest_days : int
        Number for days to ingest beginning from the start date
    configuration : str
        NWM forecast category
    json_dir: str
        Path to the directory for the kerchunk reference files

    Returns
    -------
    list
        List of paths to the single file reference json files
    """
    fs = fsspec.filesystem("gcs", anon=True)
    dates = pd.date_range(start=start_date, periods=ingest_days, freq="1d")

    lst_all_paths = []
    for dt in dates:
        dt_str = dt.strftime("%Y%m%d")
        file_path = f"{gcs_dir}/nwm.{dt_str}/{configuration}"
        lst_all_paths.extend(fs.ls(file_path))

    # Filter out the specified component
    lst_component_paths = [
        f"gs://{path}" for path in lst_all_paths if component in path
    ]

    json_paths = dask.compute(
        *[dask.delayed(gen_json)(u, fs, json_dir) for u in lst_component_paths],
        retries=1,
    )

    return json_paths


# NOTE: Including this in the creation of the multifile saves a bit of processing time,
# but adds some complexity since you need to define which variables to drop
# def pre_process(refs):
#     for k in list(refs):
#         [refs.pop(k) for var in DROP_VARS if k.startswith(var)]
#     return refs


def build_multifile_json(json_list: str, multifile_filepath: str) -> None:
    """
    Creates the multi-file kerchunk reference json

    Parameters
    ----------
    json_list : str
        List of paths to the single-file reference jsons
    multifile_filename : str
        Filepath to the output multi-file reference json
    """
    json_list = sorted(json_list)

    mzz = MultiZarrToZarr(
        json_list,
        remote_protocol="gcs",
        remote_options={"anon": True},
        concat_dims=["reference_time", "time"],
        # preprocess=pre_process,  # saves some time, but adds complexity. Omit?
    )

    mzz.translate(multifile_filepath)


def fetch_nwm(
    multifile_filepath: str,
    location_ids: np.array,
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
):
    """
    Reads in the multifile reference json, subsets the NWM data based on provided IDs
    and formats and saves the data as a parquet file

    Parameters
    ----------
    multifile_filepath : str
        Filepath to the output multi-file reference json
    location_ids : np.array
        Array specifying NWM IDs of interest
    configuration : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    """
    # Access the multifile reference file as an xarray dataset
    fs = fsspec.filesystem("reference", fo=multifile_filepath)
    m = fs.get_mapper("")
    ds = xr.open_zarr(m, consolidated=False)

    # Subset the dataset by IDs of interest
    location_ids = location_ids.astype(float)
    ds_nwm_subset = ds.sel(feature_id=location_ids)

    # For each reference time, fetch the data, format it, and save to parquet
    for ref_time in ds_nwm_subset.reference_time.values:
        # Subset by reference time
        ds_temp = ds_nwm_subset.sel(reference_time=ref_time)
        # Convert to dataframe and do some reformatting
        df_temp = ds_temp.streamflow.to_dataframe()
        df_temp.reset_index(inplace=True)
        df_temp.rename(
            columns={
                "streamflow": "value",
                "time": "value_time",
                "feature_id": "location_id",
            },
            inplace=True,
        )
        df_temp["value"] = df_temp["value"] / (0.3048**3)
        df_temp.dropna(subset=["value"], inplace=True)
        df_temp["measurement_unit"] = "ft3/s"
        df_temp["lead_time"] = df_temp["value_time"] - df_temp["reference_time"]
        df_temp["configuration"] = configuration
        df_temp["variable_name"] = variable_name
        df_temp["location_id"] = df_temp.location_id.astype(int)
        ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")
        # Save to parquet
        parquet_filepath = os.path.join(
            output_parquet_dir, f"{ref_time_str}.parquet"
        )
        df_temp.to_parquet(parquet_filepath)

        print(f"\tSaving parquet file for {ref_time_str}")


def nwm_to_parquet(
    gcs_dir: str,
    configuration: str,
    component: str,
    ingest_days: int,
    start_date: str,
    variable_name: str,
    multifile_filepath: str,
    json_dir: str,
    output_parquet_dir: str,
    location_ids: np.array,
):
    """
    Main function to download NWM data via zarr+kerchunk and save to formatted parquet files

    Parameters
    ----------
    gcs_dir : str
        GCS directory of the operational NWM data
    configuration : str
        NWM forecast category
    component : str
        Component of the NWM forecast category
    ingest_days : int
        Number for days to ingest beginning from the start date
    start_date : str
        Beginning date defining data ingestion time period
    variable_name : str
        Name of the NWM data variable to download
    multifile_filepath : str
        Name of the kerchunk combined reference file
    json_dir : str
        Path to the directory for the kerchunk reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files
    location_ids: np.array
        Array specifying NWM IDs of interest
    """

    print(f"Number of days: {ingest_days}")

    if not os.path.exists(json_dir):
        os.makedirs(json_dir)

    t1 = time.time()
    json_paths = build_single_jsons(
        gcs_dir, component, start_date, ingest_days, configuration, json_dir
    )
    print(f"Built single jsons in: {(time.time() - t1)/60:.2f} mins")

    t1 = time.time()
    build_multifile_json(json_paths, multifile_filepath)
    print(f"Built multi jsons in: {(time.time() - t1)/60:.2f} mins")

    t1 = time.time()
    fetch_nwm(
        multifile_filepath,
        location_ids,
        configuration,
        variable_name,
        output_parquet_dir,
    )
    print(f"Fetched data and saved to parquet in: {(time.time() - t1)/60:.2f} mins")

    print(f"Total elapsed: {(time.time() - t0)/60:.2f} mins")
