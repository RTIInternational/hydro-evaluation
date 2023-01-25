# package imports
import dash
from dash import html, dcc, callback, Input, Output
from utils.settings import DATA_DIR
import os

dash.register_page(
    __name__,
    path='/',
    redirect_from=['/home'],
    title='hydro_dash POC'
)

study_dirs = os.listdir(DATA_DIR)

layout = html.Div(
    [
        html.H1('Hydro Dash'),
        html.Div([
            html.Font('Select a model to review: '),
            dcc.Dropdown(study_dirs, study_dirs[1], id='ddSelectedModel')
        ]),
        html.Div(id='content')
    ]
)

@callback(Output('content', 'children'), Input('ddSelectedModel', 'value'))
def selected_study(value):
    return f'You have selected {value}'

