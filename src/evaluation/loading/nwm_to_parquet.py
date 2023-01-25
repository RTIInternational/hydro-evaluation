import os
import config
import const
import utils
from hydrotools.nwm_client import gcp as nwm
from datetime import datetime, timedelta
import pandas as pd


def fetch_nwm(reference_time: str) -> pd.DataFrame:
    # Instantiate model data service
    #  By default, NWM values are in SI units
    model_data_service = nwm.NWMDataService(cache_path=config.NWM_CACHE_H5)

    # Retrieve forecast data
    #  By default, only retrieves data at USGS gaging sites in
    #  CONUS that are used for model assimilation
    forecast_data = model_data_service.get(
        configuration="medium_range_mem1",
        reference_time=reference_time
    )

    # Look at the data
    # print(forecast_data.info(verbose=True, memory_usage='deep'))
    # print(forecast_data.memory_usage(index=True, deep=True))

    # Return the data
    return forecast_data


def nwm_to_parquet(df: pd.DataFrame, ref_time_str):
    # convert to US units
    df["value"] = df["value"]/(0.3048**3)
    df["measurement_unit"] = "ft3/s"
    df["lead_time"] = df["value_time"] - df["reference_time"]

    # Save as parquet file
    parquet_filepath = os.path.join(config.MEDIUM_RANGE_PARQUET, f"{ref_time_str}.parquet")
    utils.make_parent_dir(parquet_filepath)
    df.to_parquet(parquet_filepath)
    

def ingest_nwm():
    # Setup some criteria
    ingest_days = 20
    start_dt = datetime(2023, 1, 1) # First one is at 00Z in date
    td = timedelta(hours=6)
    number_of_forecasts = ingest_days * 4

    # Loop though forecasts, fetch and save as parquet
    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")
        print(f"Fetching NWM: {ref_time_str}")
        forecast_data = fetch_nwm(reference_time=ref_time_str)
        print(f"Fetched: {len(forecast_data)} rows")
        nwm_to_parquet(forecast_data, ref_time_str)


if __name__ == "__main__":
    ingest_nwm()
