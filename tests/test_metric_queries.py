from datetime import datetime

import pandas as pd
import geopandas as gpd
import pytest
from pydantic import ValidationError
from queries import queries
from pathlib import Path

PRIMARY_FILEPATH = Path("tests", "data", "test_study", "timeseries", "*_obs.parquet")
SECONDARY_FILEPATH = Path("tests", "data", "test_study", "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path("tests", "data", "test_study", "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path("tests", "data", "test_study", "geo", "gages.parquet")

def test_query_str():
            
    query_str = queries.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
    )
    # print(query_str)
    assert type(query_str) == str


def test_query_df():
            
    query_df = queries.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        return_query=False,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, pd.DataFrame)


def test_query_gdf():
            
    query_df = queries.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        return_query=False,
        include_geometry=True,
    )
    # print(query_df)
    assert len(query_df) == 3
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_query_gdf_2():
            
    query_df = queries.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        geometry_filepath=GEOMETRY_FILEPATH,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
        return_query=False,
        include_geometry=True,
    )
    print(query_df)
    assert len(query_df) == 9
    assert isinstance(query_df, gpd.GeoDataFrame)


def test_query_gdf_no_geom():
    with pytest.raises(ValidationError):
        query_df = queries.get_metrics(
            primary_filepath=PRIMARY_FILEPATH,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            group_by=["primary_location_id", "reference_time"],
            order_by=["primary_location_id"],
            return_query=False,
            include_geometry=True,
        )

    
def test_query_gdf_missing_group_by():
    with pytest.raises(ValidationError):
        query_df = queries.get_metrics(
            primary_filepath=PRIMARY_FILEPATH,
            secondary_filepath=SECONDARY_FILEPATH,
            crosswalk_filepath=CROSSWALK_FILEPATH,
            geometry_filepath=GEOMETRY_FILEPATH,
            group_by=["reference_time"],
            order_by=["primary_location_id"],
            return_query=False,
            include_geometry=True,
        )


if __name__ == "__main__":
    # test_query_str()
    # test_query_df()
    # test_query_gdf()
    # test_query_gdf_2()
    # test_query_gdf_no_geom()
    # test_query_gdf_missing_group_by()
    pass