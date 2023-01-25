import config
import utils
import xarray as xr
import pandas as pd
import geopandas as gpd


def get_route_links() -> pd.DataFrame:
    file = config.ROUTE_LINK_FILE
    ds = xr.open_dataset(file)
    df = ds.to_dataframe()
    df["gages"] = df["gages"].str.decode("utf-8").str.strip()
    df.rename(
        columns={
            "link": "nwm_feature_id",
            "from": "from_node",
            "to": "to_node",
            "gages": "gage_id",
            "lon": "longitude",
            "lat": "latitude"
        },
        inplace=True
    )
    return df[["nwm_feature_id", "from_node", "to_node", "longitude", "latitude", "gage_id"]]


def route_links_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    )
    return gdf


def route_link_to_parquet():
    df = get_route_links()
    gdf = route_links_to_gdf(df)
    utils.gdf_to_parquet(gdf, config.ROUTE_LINK_PARQUET)
    
    
if __name__ == "__main__":
    route_link_to_parquet()
    
    
    