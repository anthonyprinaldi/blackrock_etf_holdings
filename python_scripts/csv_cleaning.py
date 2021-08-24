import configparser as cp
import csv
import logging
from datetime import date

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from sql_methods import insert_into_sql

# define global constants
CASH_FORMAT = "XXX CASH"


def clean_blackrock_csv(csv_path: str) -> pd.DataFrame:
    """Clean csv files from blackrock holding pages

    Args:
        df (pd.DataFrame): dataframe from blackrock

    Returns:
        pd.DataFrame: cleaned dataframe to be inserted into psql
    """
    logger = logging.getLogger(__name__ + ".clean_blackrock_csv")

    index = 0
    n_skip = 0
    logger.debug("Finding number of rows to skip...")
    with open(csv_path, "r") as f:
        csv_reader = csv.reader(f, delimiter=",", quotechar='"')
        for row in csv_reader:
            if row[0] == "Ticker":
                n_skip = index
            index += 1
        logger.debug(f"Skipping {n_skip} rows in the csv file {csv_path}")
    try:
        df = pd.read_csv(csv_path, skiprows=n_skip)
        df.loc[:, "dt"] = date.today().strftime("%Y-%m-%d")
        df = df.dropna(axis=0, subset=["Ticker"])
        return df
    except Exception as e:
        logger.warning(e)

    return None


def append_stock_ids(
    df: pd.DataFrame, conn: psycopg2.extensions.connection, etf_id: str
) -> pd.DataFrame:
    """Takes in pandas data frame of holdings, adds the stocks id of the stock and the etf

    Args:
        df (pd.DataFrame): dataframe of stock holdings
        conn (psycopg2.extensions.connection): database connection object
        etf [str]: the ticker for the etf

    Returns:
        pd.DataFrame: dataframe with appended data on stock ids
    """
    logger = logging.getLogger(__name__ + ".append_stock_ids")
    logger.debug("Getting etf id...")

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

        df["etf_id"] = int(etf_id)
        logger.debug(f"ETF ID is {etf_id}")

        df["stock_id"] = np.nan

        df.reset_index(drop=True, inplace=True)

        query = """
        SELECT * FROM stocks WHERE symbol = %s
        """

        for row in df.itertuples():
            if not (
                len(str(row.Name).strip()) == len(CASH_FORMAT)
                and "CASH" in str(row.Name.strip())
            ):
                cursor.execute(query, (row.Ticker,))
                res = cursor.fetchone()
                if not (res is None):
                    df.loc[row.Index, "stock_id"] = res["id"]
                else:
                    logger.debug(f"STOCK {row.Ticker} is not in the stock table")
            else:
                logger.debug(f"Skipping over currency: {row.Ticker}")
    return df


def groupby_and_convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Sums shares over same tickers, converts columns to numeric
    and drops NA values

    Args:
        df (pd.DataFrame): raw dataframe

    Returns:
        pd.DataFrame: cleaned dataframe
    """
    logger = logging.getLogger(__name__ + ".groupby_and_convert_types")

    df = df[
        [
            "etf_id",
            "stock_id",
            "dt",
            "Shares",
            "Weight (%)",
            "Market Value",
            "Price",
        ]
    ]

    df = df.dropna(
        axis=0,
        subset=[
            "etf_id",
            "stock_id",
            "dt",
            "Shares",
            "Weight (%)",
            "Market Value",
            "Price",
        ],
    )

    try:
        df.loc[:, "Shares"] = df["Shares"].str.replace(",", "").astype(float)
    except Exception as e:
        logger.debug(f"Shares already numeric: {e}")
    try:
        df.loc[:, "Market Value"] = (
            df["Market Value"].str.replace(",", "").astype(float)
        )
    except Exception as e:
        logger.debug(f"Market Value already numeric: {e}")
    try:
        df.loc[:, "Price"] = df["Price"].str.replace(",", "").astype(float)
    except Exception as e:
        logger.debug(f"Price already numeric: {e}")
    try:
        df.loc[:, "Weight (%)"] = df["Weight (%)"] / 100
    except Exception as e:
        logger.debug(f"Weight already numeric: {e}")

    df = (
        df.groupby(["etf_id", "stock_id", "dt"])
        .agg(
            num_shares=pd.NamedAgg(column="Shares", aggfunc="sum"),
            weight=pd.NamedAgg(column="Weight (%)", aggfunc="sum"),
            market_value=pd.NamedAgg(column="Market Value", aggfunc="sum"),
            average_price=pd.NamedAgg(column="Price", aggfunc="mean"),
        )
        .reset_index()
    )

    # round values
    df.loc[:, "num_shares"] = df["num_shares"].round(2)
    df.loc[:, "market_value"] = df["market_value"].round(2)
    df.loc[:, "average_price"] = df["average_price"].round(2)
    df.loc[:, "weight"] = df["weight"].round(6)
    return df


def main() -> None:
    file_handler = logging.FileHandler("./log/csv_cleaning.log")
    file_handler.setLevel(logging.WARN)

    sys_handler = logging.StreamHandler()
    sys_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    sys_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, sys_handler])
    logger = logging.getLogger(__name__)

    logger.info("Cleaning CSVs...")
    config = cp.ConfigParser()
    config.read("./python_scripts/config.ini")
    conn = psycopg2.connect(
        host=config["psql"]["host"],
        database=config["psql"]["dbname"],
        user=config["psql"]["user"],
        password=config["psql"]["password"],
    )

    df = groupby_and_convert_types(
        append_stock_ids(
            clean_blackrock_csv("./data/temp/830.csv"),
            conn,
            "830",
        )
    )
    print(df)
    # logger.info("Inserting into table...")
    # insert_into_sql(
    #     "etf_holdings",
    #     df,
    #     conn,
    #     insert_cols=[
    #         "etf_id",
    #         "stock_id",
    #         "dt",
    #         "num_shares",
    #         "weight",
    #         "market_value",
    #         "average_price",
    #     ],
    # )

    return None


if __name__ == "__main__":
    main()
