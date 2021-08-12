import dash
from dash.dependencies import Output
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import psycopg2 as psyco
import numpy as np
from dash.dependencies import Input, Output
import configparser as cp

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    update_title=None,
    title="ETF Dashboard",
)

config = cp.ConfigParser()
config.read("./python_scripts/config.ini")
conn = psyco.connect(
    host=config["psql"]["host"],
    dbname=config["psql"]["dbname"],
    user=config["psql"]["user"],
    password=config["psql"]["password"],
    port=config["psql"]["port"],
)

# df = pd.read_sql("""SELECT * FROM etf_holdings ORDER BY dt LIMIT 100""", conn)

top_mv_shares_change = pd.read_sql(
    """
WITH mv AS (
    SELECT
        s2.symbol AS etf,
        s2.name AS etf_name,
        s1.symbol AS stock,
        s1.name AS stock_name,
        dt,
        market_value AS mv_today,
        LAG(market_value) OVER (
            PARTITION BY etf_id,
            stock_id
            ORDER BY
                dt
        ) AS mv_yesterday,
        num_shares AS shares_today,
        LAG(num_shares) OVER (
            PARTITION BY etf_id,
            stock_id
            ORDER BY
                dt
        ) AS shares_yesterday
    FROM
        etf_holdings
        LEFT JOIN stocks s1 ON etf_holdings.stock_id = s1.id
        LEFT JOIN stocks s2 ON etf_holdings.etf_id = s2.id
),
change AS (
    SELECT
        etf,
        etf_name,
        stock,
        stock_name,
        dt,
        mv_today - mv_yesterday AS market_val_change,
        shares_today - shares_yesterday AS shares_change
    FROM
        mv
    WHERE
        dt = (SELECT MAX(dt) FROM etf_holdings)
    ORDER BY
        etf,
        shares_change,
        market_val_change DESC
)
SELECT
    etf,
    etf_name,
    stock,
    stock_name,
    dt,
    shares_change,
    market_val_change
FROM
    (
        SELECT
            change.*,
            rank() OVER (
                PARTITION BY etf
                ORDER BY
                    shares_change DESC
            )
        FROM
            change
    ) mv_shares_rank
WHERE
    rank <= 5
    AND shares_change <> 0
ORDER BY
    etf,
    ABS(shares_change) DESC;

""",
    conn,
)


app.layout = html.Div(
    [
        ############################################
        # PAGE MAIN HEADER
        ############################################
        html.Br(),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(
                    html.H3("ETF Dashboard"),
                    width={"size": 12, "offset": 0},
                    style={"text-align": "center", "padding-top": "1rem"},
                ),
                html.P(
                    "Created by Anthony Rinaldi",
                    style={"font-style": "italic", "text-align": "center"},
                ),
            ],
            align="centrer",
            justify="center",
            style={"padding": "1rem", "background-color": "lightgrey"},
        ),
        html.Hr(),
        html.Br(),
        ############################################
        # AREA TO SELECT ETF
        ############################################
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Label("Please Select an ETF", html_for="etf-dropdown"),
                        dcc.Dropdown(
                            id="etf-dropdown",
                            options=[
                                {"label": etf, "value": etf}
                                for etf in sorted(top_mv_shares_change["etf"].unique())
                            ],
                            value="",
                        ),
                    ],
                    width={"size": 4},
                    style={"margin-left": "1rem"},
                ),
            ],
            align="center",
            justify="start",
            no_gutters=False,
            style={"margin-bottom": "2rem"},
        ),
        ############################################
        # DISPLAY MESSAGE ABOUT TABLE
        ############################################
        dbc.Row(
            html.P("The below table shows the one day change in ETF holding positions"),
            align="center",
            justify="start",
            style={"margin-left": "1rem"},
        ),
        ############################################
        # SHOW RESULTS OF ETF SELECTION
        ############################################
        dbc.Row(
            dbc.Col(
                children=[
                    dbc.Spinner(
                        children=[],
                        id="table-area",
                        color="primary",
                        fullscreen=True,
                        spinner_style={"align-content": "center"},
                    )
                ],
                width={"size": "10"},
            ),
            align="center",
            justify="center",
        ),
    ]
)

############################################
# HANDLING WHEN USER SELECTS ETF FOR TOP HOLDING CHANGES
############################################
@app.callback(
    [Output(component_id="table-area", component_property="children")],
    [Input(component_id="etf-dropdown", component_property="value")],
)
def filter_for_etf(etf_choice):
    dff = top_mv_shares_change.loc[top_mv_shares_change["etf"] == etf_choice, :]
    dff = dff[
        [
            "etf",
            "etf_name",
            "stock",
            "stock_name",
            "shares_change",
            "market_val_change",
            "dt",
        ]
    ]
    dff = dff.rename(
        columns={
            "etf": "ETF Symbol",
            "etf_name": "ETF Name",
            "stock": "Stock Symbol",
            "stock_name": "Stock Name",
            "dt": "Date Of Change",
            "shares_change": "Change in Shares",
            "market_val_change": "Change in Market Value (USD)",
        }
    )
    return [dbc.Table.from_dataframe(dff, striped=True, bordered=True, hover=True)]


if __name__ == "__main__":
    app.run_server(host="10.0.0.6", port="80", debug=False)
