# Import the nwm Client
from hydrotools.nwm_client import gcp as nwm
import pandas as pd
# Import the NWIS IV Client
from hydrotools.nwis_client.iv import IVDataService

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
        configuration = "medium_range_mem1",
        reference_time = "20221103T00Z"
        )

    # Look at the data
    print(forecast_data.info(memory_usage='deep'))
    print(forecast_data)

    
def fetch_usgs():
    # Retrieve data from a single site
    service = IVDataService(
        value_time_label="value_time"
    )
    observations_data = service.get(
        sites='01646500',
        startDT='2019-08-01',
        endDT='2020-08-01'
        )

    # Look at the data
    print(observations_data.head())


if __name__ == "__main__":
    fetch_nwm()
    # fetch_usgs()