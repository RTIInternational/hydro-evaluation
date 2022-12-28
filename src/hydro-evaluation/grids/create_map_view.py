import psycopg2
import psycopg2.extras

import wide_table.config as config


def create_map_view():
    """Create a detailed raster view.

    This view adds reference_time, lead_time and value_time to raster table.
    
    """
    qry = """
        --DROP VIEW forcing_medium_range_attrs;
        CREATE OR REPLACE VIEW forcing_medium_range_attrs AS (
        SELECT 
            fmra.rast,
            fmra.filename, 
            to_timestamp(
                concat(
                    split_part(fmra.filename,'.',2),
                    rtrim(ltrim(split_part(fmra.filename,'.',5), 't'), 'z') 
                ) ,'YYYYMMDDHH24'
            ) AS reference_time,
            to_timestamp(
                concat(
                    split_part(fmra.filename,'.',2),
                    rtrim(ltrim(split_part(fmra.filename,'.',5), 't'), 'z') 
                ) ,'YYYYMMDDHH24'
            )  + INTERVAL '1 hour' * ltrim(split_part(fmra.filename,'.',8),'f')::int4 AS value_time,
            ltrim(split_part(fmra.filename,'.',8),'f')::int4 AS lead_time
        FROM forcing_medium_range fmra
        );
    """

    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(qry)
            except (Exception, psycopg2.Error) as error:
                print(error)
    conn.close()


if __name__ == "__main__":
    create_map_view()