import pandas as pd
import xarray as xr
import rioxarray

ds = xr.open_dataset(
    "/home/matt/Downloads/nwm.20221001_forcing_medium_range_nwm.t00z.medium_range.forcing.f001.conus.nc",
    engine='h5netcdf',
    mask_and_scale=False,
    decode_coords="all"
)
# print(ds.crs.spatial_ref)
# df = ds.to_dataframe() #.reset_index()
# print(df["RAINRATE"])
# print(ds.variables.get("RAINRATE"))

# WKT strings extracted from NWM grids
wkt = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

# ds["RAINRATE"].rio.to_raster("nwm_grid.tif", crs=wkt)
ds["RAINRATE"].rio.to_raster("nwm_grid.tif")

# print(ds.variables.get("RAINRATE").attrs.get("proj4"))


# xds = rxr.open_rasterio(
#     "/home/matt/Downloads/nwm.20180917_forcing_medium_range_nwm.t00z.medium_range.forcing.f001.conus.nc",
#     decode_times=False
#     )
# print(xds.sel(variable="RAINRATE"))
# xds[0].variables["RAINRATE"].rio.to_raster("test.tif")
