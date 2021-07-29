CREATE TABLE stocks (
  id SERIAL UNIQUE,
  symbol TEXT NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  exchange TEXT NOT NULL,
  country TEXT,
  ipo_year INTEGER
);

CREATE TABLE etf_holdings (
    holding_id SERIAL,
    etf_id INTEGER NOT NULL,
    stock_id INTEGER NOT NULL,
    dt DATE NOT NULL,
    num_shares NUMERIC,
    weight NUMERIC,
    market_value NUMERIC,
    average_price NUMERIC,
    PRIMARY KEY (etf_id, stock_id, dt),
    CONSTRAINT fk_etf FOREIGN KEY (etf_id) REFERENCES stocks (id),
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stocks (id)
);

CREATE TABLE etf_urls (
  etf_id INTEGER NOT NULL PRIMARY KEY,
  csv_url TEXT NOT NULL,
  base_url TEXT NOT NULL,
  CONSTRAINT fk_etf FOREIGN KEY (etf_id) REFERENCES stocks (id)
);