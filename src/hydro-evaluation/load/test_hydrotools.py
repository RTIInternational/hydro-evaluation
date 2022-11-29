# Import the nwm Client
from typing import List

import config
import pandas as pd
# Import the NWIS IV Client
from hydrotools.nwis_client.iv import IVDataService
from hydrotools.nwm_client import gcp as nwm
from models import DateTimeTagType, StringTagType
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

engine = create_engine(config.CONNECTION, echo=True, future=True)


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
    return forecast_data

    
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


def df_to_evaldb(
    df: pd.DataFrame, 
    unq_cols: List[str],
    tag_cols: List[str],
):
    """Write pd.DataFrame to eval database.
    """
    # get tags that are the same for all timeseries
    get_tag_types(tag_cols)

    get_unique_tag_values(df, tag_cols)

def get_tag_types(tag_names: List[str]):
    with Session(engine) as session:
        stmt = select(StringTagType).where(StringTagType.name.in_(tag_names))

        for user in session.scalars(stmt):
            print(user)
    
    with Session(engine) as session:
        stmt = select(DateTimeTagType).where(DateTimeTagType.name.in_(tag_names))

        for user in session.scalars(stmt):
            print(user)


def get_unique_tag_values(df: pd.DataFrame, tag_names: List[str]):
    for tag_name in tag_names:
        if tag_name in config.STRING_TAG_TYPES:
            unique_values = df[tag_name].unique()
            if len(unique_values) > 1:
                raise ValueError(f"There is more than 1 unique {tag_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(f"Tag name {tag_name} does not exist in DataFrame.")
            print(f"Found {tag_name} {unique_values[0]} ")


        if tag_name in config.DATETIME_TAG_TYPES:
            unique_values = df[tag_name].unique()
            if len(unique_values) > 1:
                raise ValueError(f"There is more than 1 unique {tag_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(f"Tag name {tag_name} does not exist in DataFrame.")
            print(f"Found {tag_name} {unique_values[0]} ")







if __name__ == "__main__":
    forecast_data = fetch_nwm()
    # fetch_usgs()
    df_to_evaldb(
        forecast_data,
        unq_cols=["nwm_feature_id", "variable_name"],
        tag_cols=["reference_time", "configuration", "measurement_unit", "variable_name"]
    )
