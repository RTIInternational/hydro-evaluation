
from datetime import datetime, timedelta

from hydrotools.nwm_client import gcp as nwm
import normalized_tags.utils as utils
from normalized_tags.utils import profile


@profile
def fetch_nwm(reference_time: str,  convert_to_us: bool = True):
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
    # convert to US units
    if convert_to_us:
        forecast_data["value"] = forecast_data["value"]/(0.3048**3)
        forecast_data["measurement_unit"] = "ft3/s"

    # Look at the data
    # print(forecast_data.info(memory_usage='deep'))
    # print(forecast_data)
    return forecast_data


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
        utils.insert_df(
            forecast_data,
            unq_cols=["reference_time", "nwm_feature_id"],
            unique_name_cols = ["reference_time", "nwm_feature_id"],
            tag_cols = ["configuration", "measurement_unit", "variable_name"],
        )


if __name__ == "__main__":
    ingest_nwm()
