import time
import os
import inspect
from io import StringIO
from typing import List
from functools import wraps
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import wide_table.config as config


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


def insert_bulk(df: pd.DataFrame, table_name: str, columns: List[str]):
    """Here we are going save the dataframe in memory and use copy_from() to copy it to the table
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
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM {temp_table_name} ON CONFLICT DO NOTHING;
                    """
                )

                # Drops temporary table (I believe this step is unnecessary,
                # but tables sizes where growing without any new data modifications
                # if this command isn't executed)
                cursor.execute(f"DROP TABLE {temp_table_name}")

                # Commit everything through cursor
                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)
    conn.close()


def get_xwalk(file=None) -> pd.DataFrame:
    if file is None:
        file = str(Path(
                        inspect.getfile(inspect.currentframe())
                        ).resolve().parent.parent
                  ) + '/data/RouteLink_CONUS_NWMv2.1.6.csv'
        
    df = pd.read_csv(
        file,
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
