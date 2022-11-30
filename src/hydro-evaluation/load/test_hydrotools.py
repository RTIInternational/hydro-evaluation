# Import the nwm Client
from typing import List, Any

import config
import utils
import time
import pandas as pd
# Import the NWIS IV Client
from hydrotools.nwis_client.iv import IVDataService
from hydrotools.nwm_client import gcp as nwm
from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import Session
from models import StringTagTypes, DateTimeTagTypes, StringTags, DateTimeTags, Timeseries




engine = create_engine(
    config.CONNECTION,
    # echo=True,
    future=True
)


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
        reference_time = "20221104T00Z"
        )

    # Look at the data
    # print(forecast_data.info(memory_usage='deep'))
    # print(forecast_data)
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
    """Write pd.DataFrame to eval database."""

    # df = df[df['nwm_feature_id'] == 1180000535]

    df_tags = create_df_tags(df, tag_cols)

    groups = df.groupby(unq_cols)

    for name, group in groups:
        start = time.perf_counter()
        ts_tags = create_ts_tags(unq_cols, name)
        with Session(engine) as session:
            ts, cr = utils.get_or_create(session, Timeseries, name="_".join([str(v) for v in name]))
            group['timeseries_id'] = ts.id
            group.rename(columns={"value_time":"datetime"}, inplace=True)
            group = group[["datetime", "value", "timeseries_id"]]
            group.to_sql("values", engine, if_exists="append")
        elapsed = time.perf_counter() - start
        print(f'Time {elapsed:0.4}')

# def get_tag_types(tag_names: List[str]):
#     with Session(engine) as session:
#         stmt = select(StringTagTypes).where(StringTagTypes.name.in_(tag_names))

#         for row in session.scalars(stmt):
#             print(row)
    
#     with Session(engine) as session:
#         stmt = select(DateTimeTagTypes).where(DateTimeTagTypes.name.in_(tag_names))

#         for row in session.scalars(stmt):
#             print(row)


def create_df_tags(df: pd.DataFrame, tag_type_names: List[str]) -> List:
    """Get tags that will be applied to every timeseries in DataFrame.

    Looks in each column provided in tag_type_names and creates tags in database.

    ToDo: this pattern might not be too good, will work for now.
    

        Parameters
        ----------
            df: 
            tag_type_names:

        Returns
        -------
            tags:
    """
    tags = []
    
    for tag_type_name in tag_type_names:
        if tag_type_name in config.STRING_TAG_TYPES:
            unique_values = df[tag_type_name].unique()
            if len(unique_values) > 1:
                raise ValueError(f"There is more than 1 unique {tag_type_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(f"Tag name {tag_type_name} does not exist in DataFrame.")
            print(f"Found {tag_type_name} {unique_values[0]} ")

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session, 
                    StringTags,
                    value=str(unique_values[0]),
                    string_tag_type_name = tag_type_name
                )
                tags.append({
                    "tag_type": "string_tag",
                    "id": tag.id
                })

        if tag_type_name in config.DATETIME_TAG_TYPES:
            unique_values = df[tag_type_name].unique()
            if len(unique_values) > 1:
                raise ValueError(f"There is more than 1 unique {tag_type_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(f"Tag name {tag_type_name} does not exist in DataFrame.")
            print(f"Found {tag_type_name} {unique_values[0]} ")

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session, 
                    DateTimeTags,
                    value=pd.Timestamp(unique_values[0]),
                    datetime_tag_type_name = tag_type_name
                )
                tags.append({
                    "tag_type": "datetime_tag",
                    "id": tag.id
                })

    return tags


def create_ts_tags(tag_type_names: List[str], tag_values: List[Any]) -> List:
    """Create tags for a single timeseries

    ToDo: this pattern might not be too good, will work for now.
    

        Parameters
        ----------
            df: 
            tag_type_names:

        Returns
        -------
            tags:
    """
    if len(tag_type_names) != len(tag_values):
        raise ValueError(f"Length of tag_type_names must equal length of tag_values")
    
    tags = []

    for i, tag_type_name in enumerate(tag_type_names):
        if tag_type_name in config.STRING_TAG_TYPES:

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session, 
                    StringTags,
                    value=str(tag_values[i]),
                    string_tag_type_name = tag_type_name
                )
                tags.append({
                    "tag_type": "string_tag",
                    "id": tag.id
                })

        if tag_type_name in config.DATETIME_TAG_TYPES:

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session, 
                    DateTimeTags,
                    value=pd.Timestamp(tag_values[i]),
                    datetime_tag_type_name = tag_type_name
                )
                tags.append({
                    "tag_type": "datetime_tag",
                    "id": tag.id
                })

    return tags


if __name__ == "__main__":
    forecast_data = fetch_nwm()
    # fetch_usgs()
    df_to_evaldb(
        forecast_data,
        unq_cols=["reference_time", "nwm_feature_id", "variable_name"],
        tag_cols=["configuration", "measurement_unit"]
    )
