import configparser as cp
import logging
from logging.handlers import NTEventLogHandler

import pandas as pd
import psycopg2
import psycopg2.extras
from numpy.lib.function_base import insert
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import requests

from sql_methods import insert_into_sql


def get_csv_download_url(url_input_path: str, url_output_path: str) -> None:
    """Takes urls of etf pages and gets the link to download the csv for the etf


    Args:
        url_input_path (str): file containing list of etf website urls
        url_output_path (str): location to save csv urls for etfs
    """

    all_csv_urls = []

    logger = logging.getLogger(__name__ + ".get_csv_download_url")

    logger.info("Reading in urls...")
    with open(url_input_path, "r") as f:

        all_urls = []

        for line in f:
            all_urls.append(
                "http://www.blackrock.com"
                + line.replace("\n", "")
                + "?switchLocale=y&siteEntryPassthrough=true"
            )

    logger.info("Opening webdriver...")
    display = Display(visible=False, size=(800, 600))
    display.start()

    driver = webdriver.Chrome()

    for url in all_urls:
        try:
            driver.get(url)
            elem = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.PARTIAL_LINK_TEXT, "Download Holdings")
                )
            )
            all_csv_urls.append([url, elem.get_attribute("href")])
            logger.info(f'Added {elem.get_attribute("href")}')

        except Exception as e:
            if hasattr(e, "message"):
                logger.error(e.message)
            else:
                logger.error(e)

    logger.info(f"Saving new file at {url_output_path}")

    with open(url_output_path, "w") as f:
        for row in all_csv_urls:
            f.write(row[0] + "," + row[1] + "\n")

    logger.info("Quiting webdriver...")
    driver.quit()
    display.stop()


def insert_urls(conn: psycopg2.extensions.connection, csv_file: str) -> None:

    logger = logging.getLogger(__name__ + ".insert_urls")

    csv_urls = []
    logger.info("Reading in urls...")
    with open(csv_file, "r") as f:
        for row in f:
            row = row.replace("\n", "")
            csv_urls.append(row.split(","))

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        tickers = []
        etf_ids = []
        final_csv_urls = []
        final_base_urls = []
        logger.info("Grabbing tickers...")
        for i in range(len(csv_urls)):
            ticker = csv_urls[i][1].split("fileName=")[-1]
            ticker = ticker.split("_holdings")[0]
            query = """
                SELECT * FROM stocks
                WHERE symbol = %s
            """
            cursor.execute(query, (ticker,))
            res = cursor.fetchone()
            try:
                etf_ids.append(res["id"])
                tickers.append(ticker)
                final_csv_urls.append(csv_urls[i][1])
                final_base_urls.append(csv_urls[i][0])

                next
            except Exception as e:
                logger.warning(e)
                logger.debug("Trying to add a period")

                try:
                    # try to add a period in the second
                    # last poition and try again
                    ticker = ticker[:-1] + "." + ticker[-1]
                    cursor.execute(query, (ticker,))
                    res = cursor.fetchone()
                    etf_ids.append(res["id"])
                    tickers.append(ticker)
                    final_csv_urls.append(csv_urls[i][1])
                    final_base_urls.append(csv_urls[i][0])
                    logger.debug("Adding period worked")
                except Exception as e2:
                    logger.warning("Adding period didn't work: {e2}")

    logger.info("Inserting into table etf_urls...")
    df = pd.DataFrame(
        {"etf_id": etf_ids, "csv_url": final_csv_urls, "base_url": final_base_urls}
    )
    insert_into_sql("etf_urls", df, conn, insert_cols=["etf_id", "csv_url", "base_url"])

    return None


def download_csv(conn: psycopg2.extensions.connection) -> None:

    logger = logging.getLogger(__name__ + ".download_csv")
    logger.info("Getting all urls...")

    query = """
        SELECT csv_url, etf_id from etf_urls;
    """

    df_csvs = pd.read_sql(query, conn)
    df_csvs["Symbol"] = (
        df_csvs.csv_url.str.split("fileName=").str[-1].str.split("_holding").str[0]
    )

    for row in df_csvs.itertuples():
        r = requests.get(row.csv_url, allow_redirects=True)
        open(f"/home/pi/dev/etf_tracking/data/temp/{row.etf_id}.csv", "wb").write(
            r.content
        )
        logger.debug(f"Done: {row.Symbol}")

    return None


def main():
    file_handler = logging.FileHandler("log/holdings_scraping.log")
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

    # logger.info("Running method to save CSVs...")
    # get_csv_download_url(
    #     "/home/pi/dev/etf_tracking/data/urls.txt",
    #     "/home/pi/dev/etf_tracking/data/csv_urls.txt",
    # )

    logger.info("Inserting urls into talbe...")
    config = cp.ConfigParser()
    config.read("config.ini")
    conn = psycopg2.connect(
        host=config["psql"]["host"],
        database=config["psql"]["dbname"],
        user=config["psql"]["user"],
        password=config["psql"]["password"],
    )

    # insert_urls(conn, "/home/pi/dev/etf_tracking/data/csv_urls.txt")

    logger.info("Downloading csvs...")
    download_csv(conn)
    return None


if __name__ == "__main__":

    main()
