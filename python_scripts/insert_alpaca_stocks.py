import configparser as cp
import logging
import logging.handlers

import alpaca_trade_api as trade_api
import pandas as pd
import psycopg2

import sql_methods


def main() -> None:
    file_handler = logging.FileHandler("log/insert_alpaca_stocks.log")
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

    logger.debug("Connecting to API")
    api = trade_api.REST(
        config["alpaca"]["key"],
        config["alpaca"]["secret"],
        base_url=config["alpaca"]["url"],
    )
    logger.info("Connected to Alpaca API...")

    df = pd.DataFrame(columns=["Symbol", "Name", "Exchange", "Country", "IPO_year"])

    logger.info("Creating df for all assets...")
    for asset in api.list_assets(status="active"):
        row = pd.DataFrame(
            {
                "Symbol": [asset.symbol],
                "Name": [asset.name],
                "Exchange": [asset.exchange],
                "Country": [None],
                "IPO_year": [None],
            }
        )
        df = pd.concat([df, row])
    logger.debug("Done creating all assets df.")

    logger.info("Connecting to the psql database...")
    conn = psycopg2.connect(
        host=config["psql"]["host"],
        database=config["psql"]["dbname"],
        user=config["psql"]["user"],
        password=config["psql"]["password"],
    )

    insert_cols = ["symbol", "name", "exchange", "country", "ipo_year"]

    sql_methods.insert_into_sql("stocks", df, conn, insert_cols)

    return None


if __name__ == "__main__":
    main()
