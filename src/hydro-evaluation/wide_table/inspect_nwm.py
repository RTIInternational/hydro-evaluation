
from wide_table.utils import profile
from hydrotools.nwm_client import gcp as nwm
import pandas as pd

@profile
def fetch_nwm():
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
        reference_time="20221010T00Z"
    )

    # Look at the DataFrame info and memory usage
    print(forecast_data.info(verbose=True, memory_usage='deep'))
    print(forecast_data.memory_usage(index=True, deep=True))
    print(forecast_data)

    # Invetigate what the DataFrame looks like when `category` 
    #   columns are swapped for `object` columns.
    dfc = forecast_data.astype({
        "usgs_site_code": "object", 
        "configuration": "object", 
        "measurement_unit": "object", 
        "variable_name": "object"
        })
    print(dfc.info(verbose=True, memory_usage='deep'))
    print(dfc.memory_usage(index=True, deep=True))


if __name__ == "__main__":
    fetch_nwm()