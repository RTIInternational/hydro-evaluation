import os
import pandas as pd
import psycopg2
import psycopg2.extras
import sqlite3
import pandas as pd
import os
import sqlite_wide_table.config as config
import sqlite_wide_table.utils as utils


def insert_xwalk(df: pd.DataFrame):
    conn = sqlite3.connect(config.CONNECTION)

    cursor = conn.cursor()
    for index, row in df.iterrows():
        query = """
            INSERT INTO nwm_usgs_xwalk (nwm_feature_id, usgs_site_code) 
            VALUES (?, ?);
        """
        cursor.execute(query, (row["nwm_feature_id"], row["usgs_site_code"]))
        conn.commit()


if __name__ == "__main__":
    xwalk_data = utils.get_xwalk()
    insert_xwalk(xwalk_data)