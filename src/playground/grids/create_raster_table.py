import psycopg2
import psycopg2.extras

import grids.config as config


def create_raster_table():
    query = """
        CREATE TABLE public.forcing_medium_range (
            rid serial4 NOT NULL,
            rast public.raster NULL,
            filename text NULL,
            CONSTRAINT enforce_coverage_tile_rast CHECK (st_iscoveragetile(rast, '01000000000000000000408F400000000000408F40000000A0FF9341C100000060004C3DC100000000000000000000000000000000000000000012000F'::raster, 288, 256)),
            CONSTRAINT enforce_height_rast CHECK ((st_height(rast) = 256)),
            CONSTRAINT enforce_max_extent_rast CHECK ((st_envelope(rast) @ '01030000000100000005000000000000A0FF9341C100000060004C3DC1000000A0FF9341C1000000A0FF4B3D410000006000944141000000A0FF4B3D41000000600094414100000060004C3DC1000000A0FF9341C100000060004C3DC1'::geometry)) NOT VALID,
            CONSTRAINT enforce_nodata_values_rast CHECK ((_raster_constraint_nodata_values(rast) = '{-9999.0000000000}'::numeric[])),
            CONSTRAINT enforce_num_bands_rast CHECK ((st_numbands(rast) = 1)),
            CONSTRAINT enforce_out_db_rast CHECK ((_raster_constraint_out_db(rast) = '{f}'::boolean[])),
            CONSTRAINT enforce_pixel_types_rast CHECK ((_raster_constraint_pixel_types(rast) = '{32BF}'::text[])),
            CONSTRAINT enforce_same_alignment_rast CHECK (st_samealignment(rast, '01000000000000000000408F400000000000408F40000000A0FF9341C100000060004C3DC1000000000000000000000000000000000000000001000100'::raster)),
            CONSTRAINT enforce_scalex_rast CHECK ((round((st_scalex(rast))::numeric, 10) = round((1000)::numeric, 10))),
            CONSTRAINT enforce_scaley_rast CHECK ((round((st_scaley(rast))::numeric, 10) = round((1000)::numeric, 10))),
            CONSTRAINT enforce_srid_rast CHECK ((st_srid(rast) = 0)),
            CONSTRAINT enforce_width_rast CHECK ((st_width(rast) = 288)),
            CONSTRAINT forcing_medium_range_pkey PRIMARY KEY (rid)
        );
        CREATE INDEX enforce_spatially_unique_forcing_medium_range_rast ON public.forcing_medium_range USING btree (((rast)::geometry));
        CREATE INDEX forcing_medium_range_st_convexhull_idx ON public.forcing_medium_range USING gist (st_convexhull(rast));
    """

    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)

                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)


if __name__ == "__main__":
    create_raster_table()