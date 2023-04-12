import os
from datetime import datetime, timedelta
import time

import fsspec
import ujson   # fast json
from kerchunk.hdf import SingleHdf5ToZarr 
from kerchunk.combine import MultiZarrToZarr
import xarray as xr
import dask
from dask.distributed import Client, LocalCluster, progress
import hvplot.xarray
import pandas as pd
import numpy as np


GCS_DIR = "gcs://national-water-model"
FORECAST = "medium_range_mem1"
COMPONENT = "channel_rt"

INGEST_DAYS = 10
START_DT = "2022-12-18"
CONFIGURATION = "medium_range_mem1"
VARIABLE_NAME = "streamflow"
DROP_VARS = ["crs", "nudge", "qBtmVertRunoff", "qBucket", "qSfcLatRunoff", "velocity"]  # to exclue from the combined dataset

MULTIFILE_FILENAME = "nwm.json"
JSON_DIR = 'jsons/'
ROUTE_LINK_FILENAME = "/home/jovyan/shared/rti-eval/nwm/route_link_conus.parquet"
OUTPUT_PARQUET_DIR = "/home/jovyan/temp/parquet"


if not os.path.exists(JSON_DIR):
    os.makedirs(JSON_DIR)


def gen_json(u, fs):
    
    so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first') # args to fs.open()
    # default_fill_cache=False avoids caching data in between file chunks to lowers memory usage.

    with fs.open(u, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        p = u.split('/')
        date = p[3]
        fname = p[5]
        outf = f'{JSON_DIR}{date}.{fname}.json'
        with open(outf, 'wb') as f:
            f.write(ujson.dumps(h5chunks.translate()).encode());
    return outf


def pre_process(refs):
    for k in list(refs):
        [refs.pop(k) for var in DROP_VARS if k.startswith(var)]
    return refs
            

def build_multifile_json(json_list: str, output_fname: str):
    
    # Create the multi-file json
    json_list = sorted(json_list)

    mzz = MultiZarrToZarr(json_list,
            remote_protocol='gcs',
            remote_options={'anon':True},
            concat_dims=['reference_time', 'time'],
            preprocess=pre_process,
        )

    mzz.translate(output_fname)
    
    
def build_single_jsons():
    fs = fsspec.filesystem("gcs", anon=True)
    dates = pd.date_range(start=START_DT, periods=INGEST_DAYS, freq='1d')

    lst_all_paths = []
    for dt in dates:
        dt_str = dt.strftime("%Y%m%d")
        file_path = f"{GCS_DIR}/nwm.{dt_str}/{FORECAST}"
        lst_all_paths.extend(fs.ls(file_path))

    # Filter out the specified component
    lst_component_paths = [f"gs://{path}" for path in lst_all_paths if COMPONENT in path]
    
    json_paths = dask.compute(*[dask.delayed(gen_json)(u, fs) for u in lst_component_paths], retries=1)
    
    return json_paths


# def format_output_table(ref_time, ds_nwm_usgs, df_route_link):
#     ds_temp = ds_nwm_usgs.sel(reference_time=ref_time)
#     # Convert to dataframe
#     df_temp = ds_temp.streamflow.to_dataframe()
#     df_temp.reset_index(inplace=True)
#     # Join usgs gauge IDs
#     df_temp.set_index(["feature_id"], inplace=True)
#     df_temp = df_temp.join(df_route_link["gage_id"])
#     df_temp.reset_index(names=["nwm_feature_id"], inplace=True)
#     # Add, rename, and format some columns
#     df_temp.rename(columns={"streamflow": "value", "gage_id": "usgs_site_code", "time": "value_time"}, inplace=True)
#     df_temp["value"] = df_temp["value"]/(0.3048**3)
#     df_temp.dropna(subset=["value"], inplace=True)
#     df_temp["measurement_unit"] = "ft3/s"
#     df_temp["lead_time"] = df_temp["value_time"] - df_temp["reference_time"]
#     df_temp["configuration"] = CONFIGURATION
#     df_temp["variable_name"] = VARIABLE_NAME
#     df_temp["nwm_feature_id"] = df_temp.nwm_feature_id.astype(int)
#     ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")

#     parquet_filepath = os.path.join(OUTPUT_PARQUET_DIR, f"{ref_time_str}_sam.parquet")
#     df_temp.to_parquet(parquet_filepath)

#     print(f"\tSaving parquet file for {ref_time_str}")


def do_the_rest():
    
    fs = fsspec.filesystem("reference", fo=MULTIFILE_FILENAME)
    m = fs.get_mapper("")
    ds = xr.open_zarr(m, consolidated=False)
    
    # Read in route link file and drop rows with no usgs gauge
    df_route_link = pd.read_parquet(ROUTE_LINK_FILENAME)
    df_route_link.replace('', np.nan, inplace=True)
    df_route_link.dropna(subset=["gage_id"], inplace=True)

    # Get the gauge IDs to use for selection
    arr_nwm_usgs = df_route_link.nwm_feature_id.values.astype(float)

    # Set this for later joining
    df_route_link.set_index(["nwm_feature_id"], inplace=True)
    
    # Subset the dataset by gauge IDs
    ds_nwm_usgs = ds.sel(feature_id=arr_nwm_usgs)
    
    # Format and save the output parquet file. THIS IS SLOWER THAN THE FOR LOOP??
    # json_paths = dask.compute(*[dask.delayed(format_output_table)(ref_time, ds_nwm_usgs, df_route_link) for ref_time in ds_nwm_usgs.reference_time.values], retries=1)
    
    # TODO: Dask delayed here?
    for ref_time in ds_nwm_usgs.reference_time.values:
        # Subset by reference time
        ds_temp = ds_nwm_usgs.sel(reference_time=ref_time)
        # Convert to dataframe
        df_temp = ds_temp.streamflow.to_dataframe()
        df_temp.reset_index(inplace=True)
        # Join usgs gauge IDs
        df_temp.set_index(["feature_id"], inplace=True)
        df_temp = df_temp.join(df_route_link["gage_id"])
        df_temp.reset_index(names=["nwm_feature_id"], inplace=True)
        # Add, rename, and format some columns
        df_temp.rename(columns={"streamflow": "value", "gage_id": "usgs_site_code", "time": "value_time"}, inplace=True)
        df_temp["value"] = df_temp["value"]/(0.3048**3)
        df_temp.dropna(subset=["value"], inplace=True)
        df_temp["measurement_unit"] = "ft3/s"
        df_temp["lead_time"] = df_temp["value_time"] - df_temp["reference_time"]
        df_temp["configuration"] = CONFIGURATION
        df_temp["variable_name"] = VARIABLE_NAME
        df_temp["nwm_feature_id"] = df_temp.nwm_feature_id.astype(int)
        ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")

        parquet_filepath = os.path.join(OUTPUT_PARQUET_DIR, f"{ref_time_str}_sam.parquet")
        df_temp.to_parquet(parquet_filepath)

        print(f"\tSaving parquet file for {ref_time_str}")

    
    
if __name__ == "__main__":
    
    t0 = time.time()
    print(f"Number of days: {INGEST_DAYS}")
    
    try:
        client.close()
    except:
        pass
    client = Client(n_workers=16)
    
    t1 = time.time()
    json_paths = build_single_jsons()
    print(f"Built single jsons in: {(time.time() - t1)/60:.2f} mins")
    
    t1 = time.time()
    build_multifile_json(json_paths, MULTIFILE_FILENAME)
    print(f"Built multi jsons in: {(time.time() - t1)/60:.2f} mins")
    
    t1 = time.time()
    do_the_rest()
    print(f"Did all the rest in: {(time.time() - t1)/60:.2f} mins")
    
    
    print(f"Total elapsed: {(time.time() - t0)/60:.2f} mins")