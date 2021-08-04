import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import psycopg2 as psyco
import numpy as np

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

app.layout = html.Div(
    
    html.H1("Hello Dash")
    
)

if __name__ == "__main__":
    app.run_server(debug=True)
