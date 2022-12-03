# Import the nwm Client
from typing import List, Any

import csv
import config
import utils
import time
import pandas as pd
import os
import psycopg2
import numpy as np
import psycopg2.extras as extras
from io import StringIO

# Import the NWIS IV Client
from hydrotools.nwis_client.iv import IVDataService
from hydrotools.nwm_client import gcp as nwm
from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import Session
from models import StringTagTypes, DateTimeTagTypes, StringTags, DateTimeTags, Timeseries

param_dic = {
    "host"      : "localhost",
    "database"  : "postgres",
    "user"      : "postgres",
    "password"  : "postgrespassword"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit(1) 
    print("Connection successful")
    return conn


engine = create_engine(
    config.CONNECTION,
    # echo=True,
    future=True
)
import time
from functools import wraps
from memory_profiler import memory_usage

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
        configuration = "medium_range_mem1",
        reference_time = "20221106T00Z"
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


def create_tag_df(ts: Timeseries, tags: List[dict], tag_type: str ):

    tag_ids = [t["id"] for t in tags if t["tag_type"] ==  tag_type]

    if tag_type == "string_tag":
        return pd.DataFrame({
            "timeseries_id": ts.id,
            "string_tag_id": tag_ids 
        })
    if tag_type == "datetime_tag":
        return pd.DataFrame({
            "timeseries_id": ts.id,
            "datetime_tag_id": tag_ids 
        })


def add_tags_to_ts(ts: Timeseries, tags: List[dict]):

    string_tags_df = create_tag_df(ts, tags, tag_type="string_tag")
    datetime_tags_df = create_tag_df(ts, tags, tag_type="datetime_tag")

    string_tags_df.to_sql("timeseries_string_tag", engine, if_exists="append", index=False, method="multi", chunksize=1000)
    datetime_tags_df.to_sql("timeseries_datetime_tag", engine, if_exists="append", index=False, method="multi", chunksize=1000)


@profile
def df_to_evaldb(
    df: pd.DataFrame, 
    unq_cols: List[str],
    tag_cols: List[str],
):
    """Write pd.DataFrame to eval database.
    
    Brute force, not too smart approach.

    Around 196 seconds to load a medium_range to the DB: method=None
    Around 218 seconds to load a medium_range to the DB, index=False, method="multi", chunksize=1000
    Around 216 seconds to load a medium_range to the DB, index=False, method="multi", chunksize=10000
    Around 172 seconds to load a medium_range to the DB, index=False
    """

    df_tags = create_df_tags(df, tag_cols)

    groups = df.groupby(unq_cols)

    for name, group in groups:
        ts_tags = create_ts_tags(unq_cols, name)
        with Session(engine) as session:
            ts, cr = utils.get_or_create(session, Timeseries, name="_".join([str(v) for v in name]))
            group['timeseries_id'] = ts.id
            group.rename(columns={"value_time":"datetime"}, inplace=True)
            group = group[["datetime", "value", "timeseries_id"]]
            # group.to_sql("values", engine, if_exists="append")
            # group.to_sql("values", engine, if_exists="append", index=False, method="multi", chunksize=1000)
            group.to_sql("values", engine, if_exists="append", index=False)

            tags = [*df_tags, *ts_tags]
            add_tags_to_ts(ts, tags)


@profile
def df_to_evaldb_2(
    df: pd.DataFrame, 
    unq_cols: List[str],
    tag_cols: List[str],
):
    """Write pd.DataFrame to eval database.
    
    Try inserting into values and tags tables only once:
    102 index=False
    158 index=False, method="multi", chunksize=10000

    """
    start = time.perf_counter()

    df_tags = create_df_tags(df, tag_cols)

    groups = df.groupby(unq_cols)

    updated_groups = []
    string_tags_df = []
    datetime_tags_df = []

    for name, group in groups:
        ts_tags = create_ts_tags(unq_cols, name)
        tags = [*df_tags, *ts_tags]

        with Session(engine) as session:
            ts, cr = utils.get_or_create(session, Timeseries, name="_".join([str(v) for v in name]))

            group['timeseries_id'] = ts.id
            group.rename(columns={"value_time":"datetime"}, inplace=True)
            group = group[["datetime", "value", "timeseries_id"]]
            updated_groups.append(group)

            string_tags_df.append(create_tag_df(ts, tags, tag_type="string_tag"))
            datetime_tags_df.append(create_tag_df(ts, tags, tag_type="datetime_tag"))

    elapsed = time.perf_counter() - start
    print(f'Create tags and pre-process time   {elapsed:0.4}')
    
    start = time.perf_counter()
    merged_string_tags_df = pd.concat(string_tags_df)
    merged_string_tags_df.to_sql("timeseries_string_tag", engine, if_exists="append", index=False)

    merged_datetime_tags_df = pd.concat(datetime_tags_df)
    merged_datetime_tags_df.to_sql("timeseries_datetime_tag", engine, if_exists="append", index=False)

    updated_df = pd.concat(updated_groups)
    updated_df.to_sql("values", engine, if_exists="append", index=False)
    
    elapsed = time.perf_counter() - start
    print(f'Insert ts_to_tags and values   {elapsed:0.4}')


def copy_from_stringio(session: Session, df: pd.DataFrame, table: str, columns: List[str]):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    conn = session.connection().connection
    with conn.cursor() as cursor:
        try:
            # cursor.copy_from(buffer, table, sep=",", columns=["datetime", "value", "timeseries_id"])
            cursor.copy_from(buffer, table, sep=",", columns=columns)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data using Pandas to_sql()

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


@profile
def df_to_evaldb_3(
    df: pd.DataFrame, 
    unq_cols: List[str],
    tag_cols: List[str],
):
    """Write pd.DataFrame to eval database.

    """
    # start = time.perf_counter()

    # df_tags = create_df_tags(df, tag_cols)

    groups = df.groupby(unq_cols)
    # print(groups.groups.keys())

    # updated_groups = []
    # string_tags_df = []
    # datetime_tags_df = []

    group_names = []
    for name, group in groups:
        group_names.append(name)
    group_df = df = pd.DataFrame(group_names , columns=unq_cols)
    group_df["name"] = group_df[unq_cols].astype(str).agg('_'.join, axis=1)

    print(group_df)
    with Session(engine) as session:
        utils.insert_bulk(session, group_df["name"], "timeseries", columns=["name"])

    #     ts_tags = create_ts_tags(unq_cols, name)
    #     tags = [*df_tags, *ts_tags]

    #     with Session(engine) as session:
    #         ts, cr = utils.get_or_create(session, Timeseries, name="_".join([str(v) for v in name]))

    #         group['timeseries_id'] = ts.id
    #         group.rename(columns={"value_time":"datetime"}, inplace=True)
    #         group = group[["datetime", "value", "timeseries_id"]]
    #         updated_groups.append(group)

    #         string_tags_df.append(create_tag_df(ts, tags, tag_type="string_tag"))
    #         datetime_tags_df.append(create_tag_df(ts, tags, tag_type="datetime_tag"))

    # elapsed = time.perf_counter() - start
    # print(f'Create tags and pre-process time   {elapsed:0.4}')
    
    # merged_string_tags_df = pd.concat(string_tags_df)
    # merged_string_tags_df.to_sql("timeseries_string_tag", engine, if_exists="append", index=False)

    # merged_datetime_tags_df = pd.concat(datetime_tags_df)
    # merged_datetime_tags_df.to_sql("timeseries_datetime_tag", engine, if_exists="append", index=False)

    # updated_df = pd.concat(updated_groups)

    # start = time.perf_counter()

    # conn = connect(param_dic)
    # with Session(engine) as session:
    #     copy_from_stringio(session, updated_df, "values")

    # updated_df.to_sql("values", engine, if_exists="append", index=False)
    # updated_df.to_sql("values", engine, if_exists="append", index=False, method=psql_insert_copy)
    
    # elapsed = time.perf_counter() - start
    # print(f'Insert values {elapsed:0.4}')


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
            # print(f"Found {tag_type_name} {unique_values[0]} ")

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
            # print(f"Found {tag_type_name} {unique_values[0]} ")

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
    df_to_evaldb_3(
        forecast_data,
        unq_cols=["nwm_feature_id", "variable_name"],
        tag_cols=["configuration", "measurement_unit"]
    )
