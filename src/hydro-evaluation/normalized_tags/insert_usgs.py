from datetime import datetime, timedelta
import normalized_tags.utils as utils

import normalized_tags.utils as utils
from normalized_tags.utils import profile
from hydrotools.nwis_client.iv import IVDataService

@profile
def fetch_usgs(start_dt: str, end_dt: str):
    # get cross walk table from *.csv
    xwalk_data = utils.get_xwalk()

    # filter out gage_ids that have letters in them as not USGS
    xwalk_data = xwalk_data[~xwalk_data['usgs_site_code'].str.contains("[a-zA-Z]").fillna(False)]

    # Retrieve data from a single site
    service = IVDataService(
        value_time_label="value_time"
    )
    observations_data = service.get(
        sites=xwalk_data["usgs_site_code"],
        startDT=start_dt,
        endDT=end_dt
    )

    observations_data["configuration"] = "observed"

    # Look at the data
    # print(observations_data.info(memory_usage='deep'))
    # print(observations_data)
    # Return the data
    return observations_data


@profile
def insert_usgs(df):

    #Insert the data in the database
    utils.insert_df(
        df,
        unq_cols=["usgs_site_code", "variable_name"],
        unique_name_cols=["usgs_site_code", "variable_name"],
        tag_cols=["measurement_unit","configuration"]
    )


def ingest_usgs():
    start = datetime(2022, 10, 1)
    download_period = timedelta(days=1)
    number_of_periods = 30

    # Fetch USGS gage data in daily batches
    for p in range(number_of_periods):

        # Setup start and end date for fetch
        start_dt = (start + download_period * p)
        end_dt = (start + download_period * (p + 1))
        start_dt_str = start_dt.strftime("%Y-%m-%d")
        end_dt_str = end_dt.strftime("%Y-%m-%d")

        observations_data = fetch_usgs(
            start_dt=start_dt_str,
            end_dt=end_dt_str
        )

        # Filter out data not on the hour
        observations_data.set_index("value_time", inplace=True)
        obs = observations_data[
            observations_data.index.hour.isin(range(0, 23)) 
            & (observations_data.index.minute == 0) 
            & (observations_data.index.second == 0)
        ]
        obs.reset_index(level=0, allow_duplicates=True, inplace=True)

        # Insert to database
        insert_usgs(obs)


if __name__ == "__main__":
    ingest_usgs()