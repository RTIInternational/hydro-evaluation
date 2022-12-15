import io
import time
from io import StringIO
from typing import List, Union

import pandas as pd
import numpy as np
import normalized_tags.config as config
from functools import wraps
import psycopg2
import psycopg2.extras
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base
from psycopg2.extensions import register_adapter, AsIs
from pathlib import Path

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
def addapt_numpy_datetime64(numpy_datetime64):
    return AsIs(numpy_datetime64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.datetime64, addapt_numpy_datetime64)


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
             if tag_type == "datetime_tag":
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


@profile
def insert_df(
    df: pd.DataFrame,
    unq_cols: List[str] = ["reference_time", "nwm_feature_id"],
    unique_name_cols: List[str] = ["reference_time", "nwm_feature_id"],
    tag_cols: List[str] = ["configuration", "measurement_unit"],
):
    """Write pd.DataFrame to normalized database schema.

    This approach requires a fair bit of wrangling to get the data transformed.

    Parameters
    ----------

    unq_cols:
        Columns to use to make unique sets of values in the DataFrame, 
            or that need to be used as a unique tag.
    unique_name_cols: 
        List of fields to use to create a unique name for the timeseries. 
            Must be the same as or a subset of unq_cols.
            Note, database does not require a unique name (but it is a good idea)
    tag_cols:
        Additional columns to use for tagging that are constant across the DataFrame


    Returns
    -------
    None

    """

    updated_groups = []
    timeseries_string_tag_df = []
    timeseries_datetime_tag_df = []
    group_names = []

    string_tags = define_df_tags(df, "string_tag", tag_cols)
    datetime_tags = define_df_tags(df, "datetime_tag", tag_cols)

    # Start group timeseries
    t = time.perf_counter()
    # if len(unq_cols) == 1:
    #     groups = df.groupby(unq_cols[0])
    # else:
    #     groups = df.groupby(unq_cols)
    groups = df.groupby(unq_cols)
    for name, group in groups:
        group_names.append(name)

    group_df = pd.DataFrame(group_names, columns=unq_cols)
    group_df["name"] = group_df[unique_name_cols].astype(
        str).agg('_'.join, axis=1)
    elapsed = time.perf_counter() - t
    print(f"Group timeseries: {elapsed}")

    # Start bulk insert TS
    t = time.perf_counter()
    ts_ids = insert_bulk(
        group_df["name"], 
        "timeseries", 
        columns=["name"], 
        returning=["id"]
    )
    ts_ids = [str(r[0]) for r in ts_ids]
    elapsed = time.perf_counter() - t
    print(f"Bulk insert TS: {elapsed}")

    # Start create timeseries and timeseries tags
    t = time.perf_counter()
    ts_string_tags_dfs = []
    ts_datetime_tags_dfs = []
    for i, name_group in enumerate(groups):
        name, group = name_group
        ts_string_tags = [*string_tags, *define_ts_tags("string_tag", unq_cols, name)]
        ts_string_tags_df = pd.DataFrame(ts_string_tags)
        # if len(ts_string_tags_df) > 0:
        #     ts_string_tag_ids = insert_multi(
        #         ts_string_tags_df[["tag_type_name", "value"]],
        #         "string_tags",
        #         columns=["string_tag_type_name", "value"],
        #         returning=["id"]
        #     )
        #     ts_string_tag_ids = [str(r[0]) for r in ts_string_tag_ids]
        #     timeseries_string_tag_df.append(
        #         pd.DataFrame({
        #             "timeseries_id": ts_ids[i], 
        #             "string_tag_id": ts_string_tag_ids
        #         })
        #     )

        ts_datetime_tags = [*datetime_tags, *define_ts_tags("datetime_tag", unq_cols, name)]
        ts_datetime_tags_df = pd.DataFrame(ts_datetime_tags)
        # if len(ts_datetime_tags_df) > 0:
        #     ts_datetime_tag_ids = insert_multi(
        #         ts_datetime_tags_df[["tag_type_name", "value"]],
        #         "datetime_tags",
        #         columns=["datetime_tag_type_name", "value"],
        #         returning=["id"]
        #     )
        #     ts_datetime_tag_ids = [str(r[0]) for r in ts_datetime_tag_ids]
        #     timeseries_datetime_tag_df.append(
        #         pd.DataFrame({
        #             "timeseries_id": ts_ids[i], 
        #             "datetime_tag_id": ts_datetime_tag_ids
        #         })
        #     )

        group['timeseries_id'] = ts_ids[i]
        group.rename(columns={"value_time": "datetime"}, inplace=True)
        group = group[["datetime", "value", "timeseries_id"]]
        updated_groups.append(group)

        # TRY THIS
        ts_string_tags_df['timeseries_id'] = ts_ids[i]
        ts_string_tags_dfs.append(ts_string_tags_df)

        ts_datetime_tags_df['timeseries_id'] = ts_ids[i]
        ts_datetime_tags_dfs.append(ts_datetime_tags_df)

    merged_string_tags_df = pd.concat(ts_string_tags_dfs)
    if len(merged_string_tags_df) > 0:
        ts_string_tag_ids = insert_bulk(
            merged_string_tags_df[["tag_type_name", "value"]],
            "string_tags",
            columns=["string_tag_type_name", "value"],
            returning=["id"]
        )
        ts_string_tag_ids = [str(r[0]) for r in ts_string_tag_ids]
        merged_string_tags_df["string_tag_id"] = ts_string_tag_ids

    merged_datetime_tags_df = pd.concat(ts_datetime_tags_dfs)
    if len(merged_datetime_tags_df) > 0:
        ts_datetime_tag_ids = insert_bulk(
            merged_datetime_tags_df[["tag_type_name", "value"]],
            "datetime_tags",
            columns=["datetime_tag_type_name", "value"],
            returning=["id"]
        )
        ts_datetime_tag_ids = [str(r[0]) for r in ts_datetime_tag_ids]
        merged_datetime_tags_df["datetime_tag_id"] = ts_datetime_tag_ids

    elapsed = time.perf_counter() - t
    print(f"Create timeseries and timeseries tags: {elapsed}")

    # Merge and insert timeseries_id - string_tag_id
    t = time.perf_counter()
    if len(merged_string_tags_df) > 0:
        # merged_string_tags_df = pd.concat(timeseries_string_tag_df)
        insert_bulk(
            merged_string_tags_df[["timeseries_id", "string_tag_id"]],
            "timeseries_string_tag",
            columns=["timeseries_id", "string_tag_id"],
        )
    elapsed = time.perf_counter() - t
    print(f"Merge and insert string tags: {elapsed}")

    # Merge and insert timeseries_id - datetime_tag_id
    t = time.perf_counter()
    if len(merged_datetime_tags_df) > 0:
        # merged_datetime_tags_df = pd.concat(timeseries_datetime_tag_df)
        insert_bulk(
            merged_datetime_tags_df[["timeseries_id", "datetime_tag_id"]],
            "timeseries_datetime_tag",
            columns=["timeseries_id", "datetime_tag_id"],
        )
    elapsed = time.perf_counter() - t
    print(f"Merge and insert datetime tags: {elapsed}")

    # Merge and insert values
    t = time.perf_counter()
    updated_df = pd.concat(updated_groups)
    insert_bulk(
        updated_df[["datetime", "value", "timeseries_id"]],
        "values",
        columns=["datetime", "value", "timeseries_id"],
    )
    elapsed = time.perf_counter() - t
    print(f"Merge and insert values: {elapsed}")


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        return instance, False
    else:
        kwargs |= defaults or {}
        instance = model(**kwargs)
        try:
            session.add(instance)
            session.commit()
        except Exception:  # The actual exception depends on the specific database so we catch all exceptions. This is similar to the official documentation: https://docs.sqlalchemy.org/en/latest/orm/session_transaction.html
            session.rollback()
            instance = session.query(model).filter_by(**kwargs).one()
            return instance, False
        else:
            return instance, True


# BaseModel = declarative_base()


# def upsert_bulk(session: Session, model: BaseModel, data: io.StringIO) -> None:
#     """
#     Fast way to upsert multiple entries at once

#     Parameters
#     ----------

#     Returns
#     -------

#     """
#     table_name = model.__tablename__
#     temp_table_name = f"temp_{table_name}"

#     columns = [c.key for c in model.__table__.columns]

#     # Select only columns to be updated (in my case, all non-id columns)
#     variable_columns = [c for c in columns if c != "id"]

#     # Create string with set of columns to be updated
#     update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in variable_columns])

#     # Rewind data and prepare it for `copy_from`
#     data.seek(0)

#     conn = session.connection().connection
#     with conn.cursor() as cursor:
#         # Creates temporary empty table with same columns and types as
#         # the final table
#         cursor.execute(
#             f"""
#             CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name})
#             ON COMMIT DROP
#             """
#         )

#         # Copy stream data to the created temporary table in DB
#         cursor.copy_from(data, temp_table_name)

#         # Inserts copied data from the temporary table to the final table
#         # updating existing values at each new conflict
#         cursor.execute(
#             f"""
#             INSERT INTO {table_name}({', '.join(columns)})
#             SELECT * FROM {temp_table_name}
#             ON CONFLICT (id) DO UPDATE SET {update_set}
#             """
#         )

#         # Drops temporary table (I believe this step is unnecessary,
#         # but tables sizes where growing without any new data modifications
#         # if this command isn't executed)
#         cursor.execute(f"DROP TABLE {temp_table_name}")

#         # Commit everything through cursor
#         conn.commit()

#     conn.close()


def insert_bulk(
    df: pd.DataFrame,
    table_name: str,
    columns: List[str],
    returning: Union[List[str], None] = None
):
    """Insert a DataFrame to DB using COPY.
    
    Parameters
    ----------
    df: pd.DataFrame
    table_name: 
    columns:
    returning

    Returns
    -------

    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    temp_table_name = f"temp_{table_name}"

    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                # Creates temporary empty table with same columns and types as
                # the final table
                cursor.execute(
                    f"""
                    CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name} INCLUDING DEFAULTS)
                    ON COMMIT DROP
                    """
                )

                # Copy stream data to the created temporary table in DB
                cursor.copy_from(buffer, temp_table_name,
                                 sep=",", columns=columns)

                # Inserts copied data from the temporary table to the final table
                # updating existing values at each new conflict

                if returning:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name}({', '.join(columns)})
                        SELECT {', '.join(columns)} FROM {temp_table_name} ON CONFLICT DO NOTHING 
                        RETURNING {', '.join(returning)};
                        """
                    )
                    records = cursor.fetchall()
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name}({', '.join(columns)})
                        SELECT {', '.join(columns)} FROM {temp_table_name} ON CONFLICT DO NOTHING;
                        """
                    )
                    records = None

                if returning:
                    join_where = " AND ".join([f"tmp.{c} = tbl.{c}" for c in columns])
                    query = f"""
                        SELECT {', '.join([f"tbl.{c}" for c in returning])} FROM {temp_table_name} tmp JOIN {table_name} tbl ON {join_where};
                        """
                    cursor.execute(query)
                    records = cursor.fetchall()

                
                # Drops temporary table (I believe this step is unnecessary,
                # but tables sizes where growing without any new data modifications
                # if this command isn't executed)
                cursor.execute(f"DROP TABLE {temp_table_name}")

                # Commit everything through cursor
                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)

    conn.close()

    return records


def insert_multi(
    df: pd.DataFrame, 
    table_name: str, 
    columns: List[str], 
    returning: Union[List[str], None] = None
):
    """Insert a DataFrame to DB using COPY.
    
    Parameters
    ----------
    df: pd.DataFrame
    table_name: 
    columns:
    returning

    Returns
    -------
    """

    data = [tuple(r) for r in df.to_numpy()]
    
    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                args_str = ','.join(cursor.mogrify(f"({','.join(['%s' for c in columns])})", x).decode('utf-8') for x in data)

                # Inserts copied data from the temporary table to the final table
                # updating existing values at each new conflict
                if returning:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name}({', '.join(columns)})
                        VALUES {args_str} ON CONFLICT DO NOTHING RETURNING {', '.join(returning)};
                        """
                    )
                    records = cursor.fetchall()
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name}({', '.join(columns)})
                        VALUES {args_str} ON CONFLICT DO NOTHING;
                        """
                    )
                    records = None

                # Commit everything through cursor
                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)

    conn.close()

    return records


def get_xwalk() -> pd.DataFrame:
    file = Path(__file__).resolve().parent.parent / \
        "data/RouteLink_CONUS_NWMv2.1.6.csv",

    df = pd.read_csv(
        file[0],
        dtype={
            "nwm_feature_id": int,
            "usgs_site_code": str,
            "latitude": float,
            "longitude": float
        },
        comment='#'
    )[["nwm_feature_id", "usgs_site_code", "latitude", "longitude"]]

    # print(df.info(memory_usage='deep'))
    # print(df.head)

    return df