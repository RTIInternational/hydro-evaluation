# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import geopandas as gpd
import pandas as pd
import plotly.express as px

from dash import Dash, dcc, html

app = Dash(__name__)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

fig = px.choropleth(merged, geojson=merged.geometry, locations=merged.index, color="HUC10")
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
