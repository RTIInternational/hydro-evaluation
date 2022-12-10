
from wide_table.utils import profile, insert_bulk
from hydrotools.nwm_client import gcp as nwm
from datetime import datetime, timedelta
import pandas as pd

@profile
def fetch_nwm(reference_time: str) -> pd.DataFrame:
    # Instantiate model data service
    #  By default, NWM values are in SI units
    #  If you prefer US standard units, nwm_client can return streamflow
    #  values in cubic feet per second by setting the unit_system parameter
    #  to "US".
    # model_data_service = nwm.NWMDataService(unit_system="US")
    model_data_service = nwm.NWMDataService()

    # Retrieve forecast data
    #  By default, only retrieves data at USGS gaging sites in
    #  CONUS that are used for model assimilation
    forecast_data = model_data_service.get(
        configuration="medium_range_mem1",
        reference_time=reference_time
    )

    # Look at the data
    # print(forecast_data.info(memory_usage='deep'))
    # print(forecast_data)
    return forecast_data


@profile
def insert_nwm(df: pd.DataFrame):
    # convert to US units
    df["value"] = df["value"]/(0.3048**3)
    df["measurement_unit"] = "ft3/s"

    columns = [
        "reference_time",
        "value_time",
        "nwm_feature_id",       
        "value",
        "configuration",
        "measurement_unit",
        "variable_name"
    ]
    insert_bulk(df[columns], "nwm_data", columns=columns)


def ingest_nwm():
    start_dt = datetime(2022, 10, 1) # Starts at 20221001T00Z
    td = timedelta(hours=6)
    number_of_forecasts = 17 * 4

    for f in range(number_of_forecasts):
        reference_time = start_dt + td * f
        ref_time_str = reference_time.strftime("%Y%m%dT%HZ")
        print(f"Fetching NWM: {ref_time_str}")
        forecast_data = fetch_nwm(reference_time=ref_time_str)
        print(f"Fetched: {len(forecast_data)} rows")
        # insert_nwm(forecast_data)


if __name__ == "__main__":
    ingest_nwm()
