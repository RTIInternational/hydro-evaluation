import config
from utils import shape_to_gdf, gdf_to_parquet


def main():
    """Convert HUC10 shapefile to parquet."""
    huc10_gdf = shape_to_gdf(config.HUC10_SHP_FILEPATH).to_crs("EPSG:4326")
    gdf_to_parquet(huc10_gdf, config.HUC10_PARQUET_FILEPATH)

if __name__ == "__main__":
    main()