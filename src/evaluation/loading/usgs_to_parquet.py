import utils
import config
import os

from datetime import datetime, timedelta

from hydrotools.nwis_client.iv import IVDataService


def fetch_usgs(sites, start_dt: str, end_dt: str):
    # Retrieve data from a single site
    service = IVDataService(
        value_time_label="value_time",
        enable_cache=False
    )
    observations_data = service.get(
        sites=sites,
        startDT=start_dt,
        endDT=end_dt
    )

    # Look at the data
    # print(observations_data.info(memory_usage='deep'))
    # print(observations_data)

    # Return the data
    return observations_data


def ingest_usgs():
    start = datetime(2023, 1, 1)
    download_period = timedelta(days=1)
    number_of_periods = 11

    sites = utils.get_usgs_gages()

    # Fetch USGS gage data in daily batches
    for p in range(number_of_periods):

        # Setup start and end date for fetch
        start_dt = (start + download_period * p)
        end_dt = (start + download_period * (p + 1))
        start_dt_str = start_dt.strftime("%Y-%m-%d")
        end_dt_str = end_dt.strftime("%Y-%m-%d")

        observations_data = fetch_usgs(
            sites=sites["gage_id"],
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

        # Save as parquet file
        parquet_filepath = os.path.join(config.USGS_PARQUET, f"{start_dt.strftime('%Y%m%d')}.parquet")
        utils.make_parent_dir(parquet_filepath)
        obs.to_parquet(parquet_filepath)


if __name__ == "__main__":
    ingest_usgs()