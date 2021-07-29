import configparser as cp
import logging
import traceback
from os import listdir, remove
from os.path import isfile, join

import pandas as pd
import psycopg2
import psycopg2.extras

from csv_cleaning import (
    append_stock_ids,
    clean_blackrock_csv,
    groupby_and_convert_types,
)
from holdings_scraping import download_csv
from sql_methods import insert_into_sql


def main():

    file_handler = logging.FileHandler(
        "/home/pi/dev/etf_tracking/python_scripts/log/daily_pull.log"
    )
    file_handler.setLevel(logging.INFO)

    sys_handler = logging.StreamHandler()
    sys_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    sys_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, sys_handler])
    logger = logging.getLogger(__name__)

    config = cp.ConfigParser()
    config.read("/home/pi/dev/etf_tracking/python_scripts/config.ini")
    conn = psycopg2.connect(
        host=config["psql"]["host"],
        database=config["psql"]["dbname"],
        user=config["psql"]["user"],
        password=config["psql"]["password"],
    )

    # read in etfs to ignore
    ignore_id = []
    with open("/home/pi/dev/etf_tracking/data/ignore_non_equity_tickers.csv", "r") as f:
        for row in f:
            row = row.replace("/n", "")
            ignore_id.append(row.split(",")[0])

    logger.info("Downloading csvs...")
    download_csv(conn)

    temp_path = "/home/pi/dev/etf_tracking/data/temp"
    files = [f for f in listdir(temp_path) if isfile(join(temp_path, f))]
    logger.info("Beginning to loop over etf_id")
    for etf in files:
        etf_id = etf.split(".")[0]
        logger.info(f"Current etf: {etf_id}")
        if etf_id in ignore_id:
            logger.info("Skipped...")
            continue
        else:
            try:
                df = groupby_and_convert_types(
                    append_stock_ids(
                        clean_blackrock_csv(join(temp_path, etf)), conn, etf_id
                    )
                )
                logger.info("Inserting into table...")
                insert_into_sql(
                    "etf_holdings",
                    df,
                    conn,
                    insert_cols=[
                        "etf_id",
                        "stock_id",
                        "dt",
                        "num_shares",
                        "weight",
                        "market_value",
                        "average_price",
                    ],
                )
                logger.info(f"ETF {etf_id} successful")
                # logger.info(f"df shape: {df.shape}")
            except Exception as e:
                logger.warning(e)
                logger.warning(f"ETF {etf_id} unsuccessful")

            # append to dataframe instead of inserting in each iteration

    logger.info("Finished inserting all data")

    logger.info("Deleting all temp files...")
    for file in files:
        remove(join(temp_path, file))

    logger.info("Temporary directory cleared.")
    return None


if __name__ == "__main__":

    main()
