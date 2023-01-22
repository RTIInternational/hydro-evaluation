# Import the nwm Client
import csv
import os
import time
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from io import StringIO
from itertools import repeat
from typing import Any, List

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
# Import the NWIS IV Client
from hydrotools.nwis_client.iv import IVDataService
from hydrotools.nwm_client import gcp as nwm
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

import config
import utils
from models import (DateTimeTags, DateTimeTagTypes, StringTags, StringTagTypes,
                    Timeseries)

engine = create_engine(
    config.CONNECTION,
    # echo=True,
    future=True
)


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        # mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        # print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


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
        reference_time="20221112T00Z"
    )

    # Look at the data
    print(forecast_data.info(memory_usage='deep'))
    # print(forecast_data)
    return forecast_data


@profile
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
    print(observations_data.info(memory_usage='deep'))
    # print(observations_data.head())
    return observations_data


def create_tag_df(ts_id: str, tags: List[dict], tag_type: str):

    tag_ids = [t["id"] for t in tags if t["tag_type"] == tag_type]

    if tag_type == "string_tag":
        return pd.DataFrame({
            "timeseries_id": ts_id,
            "string_tag_id": tag_ids
        })
    if tag_type == "datetime_tag":
        return pd.DataFrame({
            "timeseries_id": ts_id,
            "datetime_tag_id": tag_ids
        })

def process_group(session, ts_id, name_group, string_tags, datetime_tags, unq_cols):
    name, group = name_group
    ts_string_tags = [*string_tags, *define_ts_tags("string_tag", unq_cols, name)]
    ts_string_tags_df = pd.DataFrame(ts_string_tags)
    # print(ts_id)
    ts_datetime_tags = [*datetime_tags, *define_ts_tags("datetime_tag", unq_cols, name)]
    ts_datetime_tags_df = pd.DataFrame(ts_datetime_tags)

    # with Session(engine) as session:
    ts_string_tag_ids = utils.insert_multi(
        session,
        ts_string_tags_df[["tag_type_name", "value"]],
        "string_tags",
        columns=["string_tag_type_name", "value"],
    )
    ts_string_tag_ids = [str(r[0]) for r in ts_string_tag_ids]

    timeseries_string_tag_df = pd.DataFrame({
        "timeseries_id": ts_id, 
        "string_tag_id": ts_string_tag_ids
    })
    utils.insert_multi(
        session,
        timeseries_string_tag_df[["timeseries_id","string_tag_id"]],
        "timeseries_string_tag",
        columns=["timeseries_id","string_tag_id"],
    )
    ts_datetime_tag_ids = utils.insert_multi(
        session,
        ts_datetime_tags_df[["tag_type_name", "value"]],
        "datetime_tags",
        columns=["datetime_tag_type_name", "value"],
    )
    ts_datetime_tag_ids = [str(r[0]) for r in ts_datetime_tag_ids]
    timeseries_datetime_tag_df = pd.DataFrame({
        "timeseries_id": ts_id, 
        "datetime_tag_id": ts_datetime_tag_ids
    })
    utils.insert_multi(
        session,
        timeseries_datetime_tag_df[["timeseries_id","datetime_tag_id"]],
        "timeseries_datetime_tag",
        columns=["timeseries_id","datetime_tag_id"],
    )

    group['timeseries_id'] = ts_id
    group.rename(columns={"value_time": "datetime"}, inplace=True)
    group = group[["datetime", "value", "timeseries_id"]]

    return group


@profile
def df_to_evaldb(
    df: pd.DataFrame,
    unq_cols: List[str] = ["reference_time",
                           "nwm_feature_id", "usgs_site_code"],
    unique_name_cols: List[str] = ["reference_time", "nwm_feature_id"],
    tag_cols: List[str] = ["configuration", "measurement_unit"],
):
    """Write pd.DataFrame to eval database.

    Parameters
    ----------

    unq_cols:
        Columns to use to make unique sets of values from, or that need to be used as a unique tag.
    unique_name_cols: 
        List of fields to use to create a unique name for the timeseries. Must be a subset of unq_cols.
        Note, database does not require a unique name.
    tag_cols:
        Additional columns to use for tagging that are constant across the DataFrame


    Returns
    -------
    None

    """

    updated_groups = []
    group_names = []

    string_tags = define_df_tags(df, "string_tag", tag_cols)
    datetime_tags = define_df_tags(df, "datetime_tag", tag_cols)

    #print(f"Group timeseries")
    t = time.perf_counter()
    groups = df.groupby(unq_cols)
    for name, group in groups:
        group_names.append(name)

    group_df = pd.DataFrame(group_names, columns=unq_cols)
    group_df["name"] = group_df[unique_name_cols].astype(
        str).agg('_'.join, axis=1)
    elapsed = time.perf_counter() - t
    print(f"Group timeseries: {elapsed}")

    #print(f"Bulk insert TS")
    t = time.perf_counter()
    with Session(engine) as session:
        ts_ids = utils.insert_bulk(
            session, group_df["name"], "timeseries", columns=["name"])
        ts_ids = [str(r[0]) for r in ts_ids]
    elapsed = time.perf_counter() - t
    print(f"Bulk insert TS: {elapsed}")

    #print(f"Create timeseries and timeseries tags")
    t = time.perf_counter()

    max_processes = max((os.cpu_count() - 2), 1)

     # Compute chunksize
    len_grps = len(groups)
    chunksize = (len_grps // max_processes) + 1

    # Retrieve data
    with Session(engine) as session:
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            updated_groups = executor.map(
                process_group, 
                repeat(session, len_grps),
                ts_ids,
                groups,
                repeat(string_tags, len_grps),
                repeat(datetime_tags, len_grps),
                repeat(unq_cols, len_grps),
                # chunksize=chunksize
                )
   
    elapsed = time.perf_counter() - t
    print(f"Create and timeseries tags: {elapsed}")

    # print(f"Merge and insert values")
    t = time.perf_counter()
    updated_df = pd.concat(updated_groups)

    with Session(engine) as session:
        utils.insert_bulk(
            session,
            updated_df[["datetime", "value", "timeseries_id"]],
            "values",
            columns=["datetime", "value", "timeseries_id"],
            returning=[]
        )
    elapsed = time.perf_counter() - t
    print(f"Merge and insert values: {elapsed}")


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
                raise ValueError(
                    f"There is more than 1 unique {tag_type_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(
                    f"Tag name {tag_type_name} does not exist in DataFrame.")
            # print(f"Found {tag_type_name} {unique_values[0]} ")

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session,
                    StringTags,
                    value=str(unique_values[0]),
                    string_tag_type_name=tag_type_name
                )
                tags.append({
                    "tag_type": "string_tag",
                    "id": tag.id
                })

        if tag_type_name in config.DATETIME_TAG_TYPES:
            unique_values = df[tag_type_name].unique()
            if len(unique_values) > 1:
                raise ValueError(
                    f"There is more than 1 unique {tag_type_name} value in DataFrame.")
            if len(unique_values) == 0:
                raise ValueError(
                    f"Tag name {tag_type_name} does not exist in DataFrame.")
            # print(f"Found {tag_type_name} {unique_values[0]} ")

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session,
                    DateTimeTags,
                    value=pd.Timestamp(unique_values[0]),
                    datetime_tag_type_name=tag_type_name
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
        raise ValueError(
            f"Length of tag_type_names must equal length of tag_values")

    tags = []

    for i, tag_type_name in enumerate(tag_type_names):
        if tag_type_name in config.STRING_TAG_TYPES:

            with Session(engine) as session:
                tag, cr = utils.get_or_create(
                    session,
                    StringTags,
                    value=str(tag_values[i]),
                    string_tag_type_name=tag_type_name
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
                    datetime_tag_type_name=tag_type_name
                )
                tags.append({
                    "tag_type": "datetime_tag",
                    "id": tag.id
                })

    return tags


def define_ts_tags(tag_type: str, tag_type_names: List[str], tag_values: List[str]):
    """
    Parameters
    ----------

    tag_type: str
        e.g., string_tag, datetime_tag


    """
    tags = []
    for i, tag_type_name in enumerate(tag_type_names):
        if tag_type == "string_tag":
            if tag_type_name in config.STRING_TAG_TYPES:
                tags.append({
                    "tag_type_name": tag_type_name,
                    "value": tag_values[i]
                })
        if tag_type == "datetime_tag":
            if tag_type_name in config.DATETIME_TAG_TYPES:
                tags.append({
                    "tag_type_name": tag_type_name,
                    "value": tag_values[i]
                })
    return tags


def define_df_tags(df: pd.DataFrame, tag_type: str, tag_type_names: List[str]) -> List:
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
        if tag_type == "string_tag":
            if tag_type_name in config.STRING_TAG_TYPES:
                unique_values = df[tag_type_name].unique()
                if len(unique_values) > 1:
                    raise ValueError(
                        f"There is more than 1 unique {tag_type_name} value in DataFrame.")
                if len(unique_values) == 0:
                    raise ValueError(
                        f"Tag name {tag_type_name} does not exist in DataFrame.")
                # print(f"Found {tag_type_name} {unique_values[0]} ")

                tags.append({
                    "tag_type_name": tag_type_name,
                    "value": unique_values[0]
                })

        if tag_type_name in config.DATETIME_TAG_TYPES:
             if tag_type == "adtetime_tag":
                unique_values = df[tag_type_name].unique()
                if len(unique_values) > 1:
                    raise ValueError(
                        f"There is more than 1 unique {tag_type_name} value in DataFrame.")
                if len(unique_values) == 0:
                    raise ValueError(
                        f"Tag name {tag_type_name} does not exist in DataFrame.")
                # print(f"Found {tag_type_name} {unique_values[0]} ")

                tags.append({
                    "tag_type_name": tag_type_name,
                    "value": unique_values[0]
                })

    return tags


if __name__ == "__main__":
    forecast_data = fetch_nwm()
    # fetch_usgs()
    df_to_evaldb(
        forecast_data
    )
