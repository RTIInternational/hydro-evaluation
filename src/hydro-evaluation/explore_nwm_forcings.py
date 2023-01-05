import pandas as pd
import rioxarray
import xarray as xr
import subprocess

ds = xr.open_dataset(
    "/home/matt/Downloads/nwm.20221001_forcing_medium_range_nwm.t00z.medium_range.forcing.f001.conus.nc",
    engine='rasterio',
    # mask_and_scale=False,
    # decode_coords="all"
)
print(ds["RAINRATE"])
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
rain_ds = ds["RAINRATE"]
rain_ds.rio.to_raster("nwm_grid.tif")
# rain_ds.to_netcdf("rainrate.nc", engine='h5netcdf')

# ds2 = xr.open_dataset(
#     "rainrate.nc",
#     engine='h5netcdf',
#     mask_and_scale=False,
#     decode_coords="all"
# )
# ds2["RAINRATE"].rio.to_raster("nwm_grid.tif", compress="LZW")

print(ds.variables.get("RAINRATE").attrs.get("esri_pe_string"))


# xds = rxr.open_rasterio(
#     "/home/matt/Downloads/nwm.20180917_forcing_medium_range_nwm.t00z.medium_range.forcing.f001.conus.nc",
#     decode_times=False
#     )
# print(xds.sel(variable="RAINRATE"))
# xds[0].variables["RAINRATE"].rio.to_raster("test.tif")


cmd = f"""
gdal_translate \
  -co COMPRESS=DEFLATE \
  -co ZLEVEL=9 \
  -co PREDICTOR=2 \
  -co TILED=YES \
  nwm_grid.tif \
  nwm_grid_cog.tif
"""
resp = subprocess.call(cmd, shell=True)

cmd = f"""
    raster2pgsql \
    -s 990000 \
    -t 256x256 \
    -I -C \
    nwm_grid_cog.tif \
    nwm_grid_cog \
    > raster.sql
"""
resp = subprocess.call(cmd, shell=True)