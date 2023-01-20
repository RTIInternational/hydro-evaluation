import duckdb
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt


# cursor = duckdb.connect()
# data = cursor.execute("""
#     SELECT catchment_id, value_time, value FROM 
#         read_parquet('20221001T00Z.parquet') 
#     WHERE 
#         reference_time = '2022-10-01T00:00:00' 
#     AND 
#         catchment_id = 1016000606;
# """).fetchall()
# df = pd.DataFrame(data, columns=["catchment_id", "value_time", "value"] )
# print(df)

# df = duckdb.query("""
#     SELECT distinct(catchment_id) FROM 
#         read_parquet('20221001T00Z.parquet');
# """).to_df()
# print(df)

# df = duckdb.query("""
#     SELECT * FROM 
#         read_parquet('map/data/forcing_analysis_assim/*.parquet') 
#     WHERE
#         catchment_id = 1016000606;
# """).to_df()
# print(df)

#-------------------------------------------------------------------
df = duckdb.query("""
    SELECT 
        forecast.catchment_id, 
        forecast.reference_time, 
        forecast.value_time, 
        --forecast.value as forecast_value, 
        --observed.value as observed_value,
        forecast.value - observed.value as delta 
    FROM 
        read_parquet('map/data/forcing_medium_range/20221001T00Z.parquet') forecast
    INNER JOIN 
        read_parquet('map/data/forcing_analysis_assim/*.parquet') observed 
    ON forecast.catchment_id = observed.catchment_id AND forecast.value_time = observed.value_time
    WHERE 
        reference_time = '2022-10-01T00:00:00' --AND forecast.value_time < '2022-10-02'
    --GROUP BY 
        --forecast.catchment_id, forecast.reference_time
        ;
""").to_df()
print(df)

shp_filepath = "/home/matt/wbdhu10_conus.shp"
gdf = gpd.GeoDataFrame.from_file(shp_filepath)

gdf_map = gdf.merge(df, left_on="huc10", right_on="catchment_id")

gdf_map = gdf_map.loc[gdf_map["catchment_id"].str.startswith("03")]

for k,v in gdf_map.groupby("value_time"):
    # plt.plot(v["value_time"], v["bias"])
    v.plot("delta", legend=True)
    plt.savefig(f"map/{k}.png")
    plt.close()

#-------------------------------------------------------------------
# df = duckdb.query("""
#     SELECT reference_time, catchment_id, sum(value) as sum FROM 
#         read_parquet('map/data/forcing_medium_range*.parquet')
#     --WHERE 
#          --reference_time = '2022-10-01T06:00:00' 
#     GROUP BY catchment_id, reference_time;
# """).to_df()
# print(df)

# shp_filepath = "/home/matt/wbdhu10_conus.shp"
# gdf = gpd.GeoDataFrame.from_file(shp_filepath)

# gdf_map = gdf.merge(df, left_on="huc10", right_on="catchment_id")

# for k,v in gdf_map.groupby("reference_time"):
#     # plt.plot(v["value_time"], v["bias"])
#     v.plot("sum", legend=True)
#     plt.savefig(f"map/{k}.png")

#------------------------------------------------------------------
