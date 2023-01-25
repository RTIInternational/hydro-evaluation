import pandas as pd
import xarray as xr

# ds = xr.open_dataset(
#     "data/RouteLink_CONUS.nc",
#     # decode_cf=True
#     # engine='h5netcdf',
#     # mask_and_scale=False
# )
# print(ds)
# df = ds.to_dataframe()
# print(df.info())
# print(df)
# df["gages"] = df["gages"].str.decode("utf-8")
# print(df.loc[df["gages"].str.strip() != ""][["link","gages"]])

df = pd.read_csv("data/NWM_features_info_conus_2_1.csv")
print(df)