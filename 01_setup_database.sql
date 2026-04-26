
CREATE DATABASE IF NOT EXISTS superstore_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE superstore_db;	

DROP TABLE IF EXISTS superstore_raw;

CREATE TABLE superstore_raw (
    row_id        VARCHAR(20),
    order_id      VARCHAR(30),
    order_date    VARCHAR(20),
    ship_date     VARCHAR(20),
    ship_mode     VARCHAR(50),
    customer_id   VARCHAR(20),
    customer_name VARCHAR(100),
    segment       VARCHAR(30),
    city          VARCHAR(60),
    state         VARCHAR(60),
    postal_code   VARCHAR(20),
    region        VARCHAR(20),
    category      VARCHAR(50),
    sub_category  VARCHAR(50),
    product_id    VARCHAR(20),
    product_name  VARCHAR(200),
    quantity      VARCHAR(10),
    discount      VARCHAR(10),
    unit_price    VARCHAR(20),
    sales         VARCHAR(20),
    profit        VARCHAR(20)
);


LOAD DATA LOCAL INFILE '/path/to/superstore_raw.csv'
INTO TABLE superstore_raw
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;   -- skip header baris pertama


SELECT COUNT(*) AS total_rows_imported FROM superstore_raw;



SELECT * FROM superstore_raw LIMIT 5;
