# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import duckdb

app = Dash(__name__)

def parquet_to_gdf(parquet_filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf

parquet_filepath = "/home/matt/wbdhu10_conus.parquet"

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
gdf = parquet_to_gdf(parquet_filepath)

df = duckdb.query("""
    SELECT 
        forecast.catchment_id, 
        forecast.reference_time, 
        --forecast.value_time, 
        --forecast.value as forecast_value, 
        --observed.value as observed_value,
        sum(forecast.value - observed.value) as delta 
    FROM 
        read_parquet('map/data/forcing_medium_range/20221001T00Z.parquet') forecast
    INNER JOIN 
        read_parquet('map/data/forcing_analysis_assim/*.parquet') observed 
    ON forecast.catchment_id = observed.catchment_id AND forecast.value_time = observed.value_time
    WHERE 
        reference_time = '2022-10-01T00:00:00' --AND forecast.value_time < '2022-10-02'
    GROUP BY 
        forecast.catchment_id, forecast.reference_time
        ;
""").to_df()
print(df)

shp_filepath = "/home/matt/wbdhu10_conus.shp"
gdf = gpd.GeoDataFrame.from_file(shp_filepath)

gdf_map = gdf.merge(df, left_on="huc10", right_on="catchment_id")

gdf_map = gdf_map.loc[gdf_map["catchment_id"].str.startswith("03")]
print(gdf_map)

fig = px.choropleth(gdf_map, geojson=gdf_map.geometry, locations=gdf_map.index, color="delta")
fig.update_geos(fitbounds="locations", visible=False)

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
