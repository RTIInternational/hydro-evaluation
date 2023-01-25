import pandas as pd
import xarray as xr
import rioxarray as rxr

ds = xr.open_dataset(
    "/home/matt/Downloads/spatialweights_CONUS_LongRange.nc",
    # decode_cf=True
    engine='h5netcdf',
    # mask_and_scale=False
)
# print(ds)
# print(ds.sel(polyid = 41000611).to_dataframe())

df = ds[['IDmask','weight','regridweight','i_index','j_index']].to_dataframe()
ds2 = df.to_xarray()
print(ds2)
ds2.to_netcdf("cross_walk.nc", engine='h5netcdf',)
print(df)

# df = ds.to_dataframe() #.reset_index()
# print(df)

# print(df["RAINRATE"])
# print(ds.variables.get("RAINRATE"))

# ds["RAINRATE"].rio.to_raster("nwm_grid.tif")

# print(ds.variables.get("RAINRATE").attrs.get("proj4"))


# xds = rxr.open_rasterio(
#     "/home/matt/Downloads/nwm.20180917_forcing_medium_range_nwm.t00z.medium_range.forcing.f001.conus.nc",
#     decode_times=False
#     )
# print(xds.sel(variable="RAINRATE"))
# xds[0].variables["RAINRATE"].rio.to_raster("test.tif")
