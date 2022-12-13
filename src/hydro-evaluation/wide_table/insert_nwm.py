
from wide_table.utils import profile, insert_bulk
from hydrotools.nwm_client import gcp as nwm
from datetime import datetime, timedelta
import pandas as pd

@profile
def fetch_nwm(reference_time: str) -> pd.DataFrame:
    # Instantiate model data service
    #  By default, NWM values are in SI units
    model_data_service = nwm.NWMDataService()

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


@profile
def insert_nwm(df: pd.DataFrame):
    # convert to US units
    df["value"] = df["value"]/(0.3048**3)
    df["measurement_unit"] = "ft3/s"

    # Define columns to insert
    columns = [
        "reference_time",
        "value_time",
        "nwm_feature_id",       
        "value",
        "configuration",
        "measurement_unit",
        "variable_name"
    ]

    # Insert the data
    insert_bulk(df[columns], "nwm_data", columns=columns)


def ingest_nwm():
    
    # Setup some criteria
    ingest_days = 20
    start_dt = datetime(2022, 10, 20) # First one is at 00Z in date
    td = timedelta(hours=6)
    number_of_forecasts = ingest_days * 4

    # Loop though forecasts, fetch and insert
    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")
        print(f"Fetching NWM: {ref_time_str}")
        forecast_data = fetch_nwm(reference_time=ref_time_str)
        print(f"Fetched: {len(forecast_data)} rows")
        insert_nwm(forecast_data)


if __name__ == "__main__":
    ingest_nwm()
