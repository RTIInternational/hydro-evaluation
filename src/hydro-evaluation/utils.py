import io
from io import StringIO
from typing import List

import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
def addapt_numpy_datetime64(numpy_datetime64):
    return AsIs(numpy_datetime64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.datetime64, addapt_numpy_datetime64)

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


BaseModel = declarative_base()


def upsert_bulk(session: Session, model: BaseModel, data: io.StringIO) -> None:
    """
    Fast way to upsert multiple entries at once

    Parameters
    ----------

    Returns
    -------

    """
    table_name = model.__tablename__
    temp_table_name = f"temp_{table_name}"

    columns = [c.key for c in model.__table__.columns]

    # Select only columns to be updated (in my case, all non-id columns)
    variable_columns = [c for c in columns if c != "id"]

    # Create string with set of columns to be updated
    update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in variable_columns])

    # Rewind data and prepare it for `copy_from`
    data.seek(0)

    conn = session.connection().connection
    with conn.cursor() as cursor:
        # Creates temporary empty table with same columns and types as
        # the final table
        cursor.execute(
            f"""
            CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name})
            ON COMMIT DROP
            """
        )

        # Copy stream data to the created temporary table in DB
        cursor.copy_from(data, temp_table_name)

        # Inserts copied data from the temporary table to the final table
        # updating existing values at each new conflict
        cursor.execute(
            f"""
            INSERT INTO {table_name}({', '.join(columns)})
            SELECT * FROM {temp_table_name}
            ON CONFLICT (id) DO UPDATE SET {update_set}
            """
        )

        # Drops temporary table (I believe this step is unnecessary,
        # but tables sizes where growing without any new data modifications
        # if this command isn't executed)
        cursor.execute(f"DROP TABLE {temp_table_name}")

        # Commit everything through cursor
        conn.commit()

    conn.close()


def insert_bulk(session: Session, df: pd.DataFrame, table_name: str, columns: List[str], returning: List[str] = ["id"]):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    temp_table_name = f"temp_{table_name}"    

    conn = session.connection().connection
    with conn.cursor() as cursor:
        # Creates temporary empty table with same columns and types as
        # the final table
        cursor.execute(
            f"""
            CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name} INCLUDING DEFAULTS)
            ON COMMIT DROP
            """
        )

        # Copy stream data to the created temporary table in DB
        cursor.copy_from(buffer, temp_table_name, sep=",", columns=columns)

        # Inserts copied data from the temporary table to the final table
        # updating existing values at each new conflict
        if len(returning) > 0:
            cursor.execute(
                f"""
                INSERT INTO {table_name}({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {temp_table_name} ON CONFLICT DO NOTHING RETURNING {', '.join(returning)};
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

        # Drops temporary table (I believe this step is unnecessary,
        # but tables sizes where growing without any new data modifications
        # if this command isn't executed)
        cursor.execute(f"DROP TABLE {temp_table_name}")

        # Commit everything through cursor
        conn.commit()

    conn.close()

    return records


def insert_multi(session: Session, df: pd.DataFrame, table_name: str, columns: List[str], returning: List[str] = ["id"]):
    """

    """

    # tups = df.itertuples(index=False, name=None)
    data = [tuple(r) for r in df.to_numpy()]
    
    conn = session.connection().connection
    with conn.cursor() as cursor:
        args_str = ','.join(cursor.mogrify(f"({','.join(['%s' for c in columns])})", x).decode('utf-8') for x in data)

        # Inserts copied data from the temporary table to the final table
        # updating existing values at each new conflict
        if len(returning) > 0:
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

    conn.close()

    return records