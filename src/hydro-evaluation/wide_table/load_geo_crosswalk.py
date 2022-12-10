import os
import pandas as pd
import psycopg2
import psycopg2.extras
import pandas as pd
import os
import wide_table.config as config
import wide_table.utils as utils


def insert_xwalk(df: pd.DataFrame):
    conn = psycopg2.connect(config.CONNECTION)

    for index, row in df.iterrows():
        with conn:
            with conn.cursor() as cursor:
                try:
                    # geometry column
                    query = """
                        INSERT INTO nwm_usgs_xwalk (nwm_feature_id, usgs_site_code, geom) 
                        VALUES (%s, %s, ST_Point(%s, %s) );
                    """
                    cursor.execute(query, (row["nwm_feature_id"], row["usgs_site_code"], row["longitude"], row["latitude"]))

                    conn.commit()
                except (Exception, psycopg2.Error) as error:
                    print(error)


if __name__ == "__main__":
    xwalk_data = utils.get_xwalk()
    insert_xwalk(xwalk_data)