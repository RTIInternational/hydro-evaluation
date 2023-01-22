import fsspec
import numpy as np
import xarray as xr
import pandas as pd
from google.cloud import storage

uriname = f'gcs://national-water-model/nwm.20180917/medium_range/nwm.t00z.medium_range.channel_rt.f003.conus.nc'

open_file = fsspec.open(uriname)

if hasattr(open_file, "open"):
    open_file = open_file.open()
ds = xr.open_dataset(open_file)

with fsspec.open(uriname, mode="rb", anon=True) as file:
    print(file)
    # ds = xr.load_dataset(file, engine='h5netcdf')