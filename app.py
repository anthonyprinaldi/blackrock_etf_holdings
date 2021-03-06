import argparse
import configparser as cp
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly.express as px
import psycopg2 as psyco
import psycopg2.extensions
from dash.dependencies import Input, Output

# time for thread to update database values
UPDATE_HOUR = 11
DATABASE_URL = os.environ["DATABASE_URL"]

finished = False

# parser = argparse.ArgumentParser("PAT or PROD server")
# parser.add_argument(
#     "location",
#     nargs=1,
#     help="location of host IP",
#     choices=["PAT", "PROD"],
# )

# opts = parser.parse_args()
hosts = {"PAT": "127.0.0.1", "PROD": "10.0.0.6"}

# create function to be called on ctrl + c
def exit_handler(signum, frame):
    global finished
    finished = True
    print("Exiting Program...", flush=True)
    exit(0)


# map the above function to ctrl + c
signal.signal(signal.SIGINT, exit_handler)


def check_for_finished_pull() -> bool:
    """check if the daily_pull.sh script has finished

    Returns:
        bool: [True if the last line in the log file is DONE, else False]
    """
    with open("./bash/logs/cron.log", "r") as f:
        lines = f.readlines()
    if lines[-1].strip() == "DONE":
        return True
    else:
        return False


def connect_psql() -> psycopg2.extensions.connection:
    """creates a connection object for psql database

    Returns:
        psycopg2.extensions.connection: [etf_tracking database connection]
    """
    config = cp.ConfigParser()
    config.read("./python_scripts/config.ini")
    # conn = psyco.connect(
    #     host=config["psql"]["host"],
    #     dbname=config["psql"]["dbname"],
    #     user=config["psql"]["user"],
    #     password=config["psql"]["password"],
    #     port=config["psql"]["port"],
    # )
    conn = psyco.connect(DATABASE_URL)
    return conn


# df = pd.read_sql("""SELECT * FROM etf_holdings ORDER BY dt LIMIT 100""", conn)
def get_new_top_changes() -> int:
    """[Update the global variable top_mv_shares_change with new data on a daily basis]

    Returns:
        int: [0 is success, else -1]
    """
    conn = connect_psql()
    global top_mv_shares_change
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
        WHERE dt = (SELECT MAX(dt) FROM etf_holdings)
        OR dt = (SELECT MAX(dt) FROM etf_holdings WHERE dt != (SELECT MAX(dt) FROM etf_holdings))
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
    conn.close()
    print(f"Data pulled at {datetime.now()}", flush=True)
    return 0


def get_new_top_changes_at(update_at: int = UPDATE_HOUR) -> None:
    """Updates the global top_mv_shares_change at certain time of day

    Args:
        time (int, optional): [Time of day to update data]. Defaults to UPDATE_HOUR.

    Returns:
        [None]: [No return >> updates global variables]
    """
    global finished
    while not finished:
        if datetime.now().hour == update_at:
            if check_for_finished_pull():
                try:
                    get_new_top_changes()
                    print(f"Data pushed at {datetime.now()}", flush=True)
                    print("Sleeping for 1 hour...")
                    time.sleep(3_600)
                except Exception as e:
                    print(f"Exception encountered: {e}\nTrying again...")
            else:
                print("Pull not finished yet...\nSleeping for 10min...")
                time.sleep(600)
        elif datetime.now().hour + 1 == update_at:
            # sleep for 10 minutes if we are an hour away from the desired time
            print("Sleeping for 10min...", flush=True)
            time.sleep(600)
        else:
            # sleep for 30 min if we are not close to the desired time
            print("Sleeping for 30min...", flush=True)
            time.sleep(1_800)


def make_layout():
    """Make the layout of the dash app

    Returns:
        [type]: [layout of the dash app]
    """
    return html.Div(
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
                                    for etf in sorted(
                                        top_mv_shares_change["etf"].unique()
                                    )
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
                html.P(
                    "The below table shows the one day change in ETF holding positions"
                ),
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


app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    update_title=None,
    title="ETF Dashboard",
)
server = app.server

# load in the data
get_new_top_changes()

# make the layout of the app
app.layout = make_layout

# add extra thread for updating the data
executor = ThreadPoolExecutor(max_workers=1)
future = executor.submit(get_new_top_changes_at)


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


app.index_string = app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        <!-- Global site tag (gtag.js) - Google Analytics -->
            <script async src="https://www.googletagmanager.com/gtag/js?id=UA-204885915-1"></script>
            <script>
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());

            gtag('config', 'UA-204885915-1');
            </script>
        <!-- End Global Google Analytics -->
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

if __name__ == "__main__":
    app.run_server(
        # host=hosts[opts.location[0]], port="80", debug=False
        host="127.0.0.1",
        port="80",
        debug=False,
    )  # change this back to 10.0.0.6 for PROD
    future.result()
