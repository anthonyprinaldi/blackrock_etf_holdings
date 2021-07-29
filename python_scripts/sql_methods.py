import configparser as cp
import logging
import logging.handlers

import alpaca_trade_api as trade_api
import pandas as pd
import psycopg2


def insert_into_sql(
    table_name: str,
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
    insert_cols: list,
    on_conflict: str = "DO NOTHING",
) -> None:
    """Insert values into a psql table

    Args:
        table_name (str): name for table to be inserted with values
        df (pd.DataFrame): dataframe with values to insert into table
        conn (psycopg2.extensions.connection): connection for database
        insert_cols (list): list of columns to get inputed into table
        on_conflict (str): how to handle conflicts in insert
    """
    logger = logging.getLogger(__name__ + ".insert_into_sql")

    # get cursor from connection
    with conn.cursor() as cursor:

        # join table columns to be appropriate for query
        table_cols_sql = "(" + ", ".join(insert_cols) + ")"

        # change NA values to None
        df = df.where(df.notnull(), None)

        # turn the dataframe into a list of tuples - needed for sql
        # values_for_insert = [
        #    f"('{sym}', '{name}', '{ex}', '{country}', {ipo})"
        #    for sym, name, ex, country, ipo in zip(
        #        df["Cleaned Symbol"],
        #        df["Name"],
        #        df["Exchange"],
        #        df["Country"],
        #        df["IPO Year"],
        #    )
        # ]
        # turn the above list into a string
        # values = ", ".join(values_for_insert)

        logger.info("Starting to insert values...")

        # get the number of fillers needed
        str_fill = (" %s," * df.shape[1])[:-1]

        try:
            for row in df.itertuples(index=False, name=None):

                query = f"""
                    INSERT INTO
                        {table_name}{table_cols_sql}
                    VALUES
                        ({str_fill})
                    ON CONFLICT {on_conflict};
                """
                cursor.execute(query, row)

        except Exception as e:
            logger.info(e)
            logger.info(row)
            conn.rollback()

        else:
            logger.debug("Done inserting values.")
            conn.commit()
            logger.info("Changes commited. Closing connection...")
