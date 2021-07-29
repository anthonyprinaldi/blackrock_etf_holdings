import configparser as cp
import logging
import logging.handlers

import numpy as np
import pandas as pd
import psycopg2

import sql_methods


def main() -> None:
    file_handler = logging.FileHandler("log/insert_etf_stocks.log")
    file_handler.setLevel(logging.DEBUG)

    sys_handler = logging.StreamHandler()
    sys_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    sys_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, sys_handler])
    logger = logging.getLogger(__name__)

    logger.info("Starting Program...")

    logger.info("Importing Credentials...")
    config = cp.ConfigParser()
    config.read("config.ini")

    logger.debug("Reading file...")

    df = pd.read_csv("../data/all_etfs.csv")
    df.rename({"Ticker": "Symbol"}, inplace=True, axis=1)
    df = df[["Symbol", "Name", "IPO Date"]]

    logger.info("Connecting to the psql database...")
    conn = psycopg2.connect(
        host=config["psql"]["host"],
        database=config["psql"]["dbname"],
        user=config["psql"]["user"],
        password=config["psql"]["password"],
    )
    # replace nan values with none
    df.replace([np.nan], [None], inplace=True)

    insert_cols = ["symbol", "name", "ipo_year"]
    sql_methods.insert_into_sql("stocks", df, conn, insert_cols)

    return None


if __name__ == "__main__":
    main()
