--  Apa aja yang di cleaning :
--  1. Audit kualitas data (cek masalah sebelum fix)
--  2. Buat staging table 
--  3. Hapus duplikat
--  4. Handle NULL / missing values
--  5. Standardisasi tipe data & format
--  6. Deteksi & tangani outlier
--  7. Validasi akhir


USE superstore_db;


-- BAGIAN 1 : AUDIT KUALITAS DATA

-- 1a. Total baris & gambaran umum
SELECT
    COUNT(*)AS total_rows,
    COUNT(DISTINCT order_id)AS unique_orders,
    COUNT(DISTINCT customer_id)AS unique_customers,
    COUNT(DISTINCT product_id)AS unique_products,
    MIN(order_date)AS earliest_order,
    MAX(order_date)AS latest_order
FROM superstore_raw;

-- 1b. Cek missing values per kolom
SELECT
    SUM(order_id IS NULL OR order_id = '') AS null_order_id,
    SUM(order_date IS NULL OR order_date = '') AS null_order_date,
    SUM(customer_id IS NULL OR customer_id = '') AS null_customer_id,
    SUM(postal_code IS NULL OR postal_code = '') AS null_postal_code,
    SUM(product_name IS NULL OR product_name = '') AS null_product_name,
    SUM(sales IS NULL OR sales = '') AS null_sales,
    SUM(profit IS NULL OR profit= '') AS null_profit,
    SUM(discount IS NULL OR discount= '') AS null_discount
FROM superstore_raw;

-- 1c. Cek duplikat exact (semua kolom sama)
SELECT
    order_id, product_id, order_date, customer_id,
    COUNT(*) AS jumlah_duplikat
FROM superstore_raw
GROUP BY order_id, product_id, order_date, customer_id
HAVING COUNT(*) > 1
ORDER BY jumlah_duplikat DESC
LIMIT 20;

-- 1d. Cek nilai aneh di kolom numerik
SELECT
    MIN(CAST(sales AS DECIMAL(12,2))) AS min_sales,
    MAX(CAST(sales AS DECIMAL(12,2))) AS max_sales,
    MIN(CAST(profit AS DECIMAL(12,2))) AS min_profit,
    MAX(CAST(profit AS DECIMAL(12,2))) AS max_profit,
    MIN(CAST(discount AS DECIMAL(5,2))) AS min_discount,
    MAX(CAST(discount AS DECIMAL(5,2)))AS max_discount,
    MIN(CAST(quantity AS UNSIGNED)) AS min_qty,
    MAX(CAST(quantity AS UNSIGNED))AS max_qty
FROM superstore_raw;

-- 1e. Cek kategori tidak konsisten (typo, case berbeda)
SELECT DISTINCT category FROM superstore_raw ORDER BY 1;
SELECT DISTINCT sub_category FROM superstore_raw ORDER BY 1;
SELECT DISTINCT segment FROM superstore_raw ORDER BY 1;
SELECT DISTINCT region FROM superstore_raw ORDER BY 1;
SELECT DISTINCT ship_mode FROM superstore_raw ORDER BY 1;

-- 1f. Cek format tanggal
SELECT order_date
FROM superstore_raw
WHERE STR_TO_DATE(order_date, '%Y-%m-%d') IS NULL AND order_date != ''
LIMIT 10;

-- BAGIAN 2 : BUAT STAGING TABLE

-- Staging table dengan tipe data yang sudah benar
DROP TABLE IF EXISTS superstore_staging;

CREATE TABLE superstore_staging (
    row_id        INT,
    order_id      VARCHAR(30)          NOT NULL,
    order_date    DATE,
    ship_date     DATE,
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
    quantity      TINYINT UNSIGNED,
    discount      DECIMAL(5,2),
    unit_price    DECIMAL(10,2),
    sales         DECIMAL(12,2),
    profit        DECIMAL(12,2),

    -- Kolom tambahan untuk analisis
    profit_margin    DECIMAL(8,4) GENERATED ALWAYS AS
                     (CASE WHEN sales != 0 THEN profit / sales ELSE 0 END) STORED,
    order_year       YEAR GENERATED ALWAYS AS (YEAR(order_date))  STORED,
    order_month      TINYINT GENERATED ALWAYS AS (MONTH(order_date)) STORED,
    order_quarter    TINYINT GENERATED ALWAYS AS (QUARTER(order_date)) STORED,
    ship_duration    TINYINT GENERATED ALWAYS AS (DATEDIFF(ship_date, order_date)) STORED,

    -- Timestamps untuk audit
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (row_id),
    INDEX idx_order_id(order_id),
    INDEX idx_customer(customer_id),
    INDEX idx_order_date(order_date),
    INDEX idx_category(category, sub_category),
    INDEX idx_region(region)
);


-- BAGIAN 3 : INSERT KE STAGING 


-- Lihat dulu berapa duplikat row_id yang ada
SELECT row_id, COUNT(*) AS jumlah
FROM superstore_raw
GROUP BY row_id
HAVING COUNT(*) > 1
ORDER BY jumlah DESC;

-- INSERT dengan deduplikasi via ROW_NUMBER()
INSERT INTO superstore_staging (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment,
    city, state, postal_code, region,
    category, sub_category, product_id, product_name,
    quantity, discount, unit_price, sales, profit
)
SELECT
    CAST(row_id   AS UNSIGNED)                    AS row_id,
    TRIM(order_id)                                AS order_id,
    STR_TO_DATE(TRIM(order_date),  '%Y-%m-%d')   AS order_date,
    STR_TO_DATE(TRIM(ship_date),   '%Y-%m-%d')   AS ship_date,
    TRIM(ship_mode)                               AS ship_mode,
    TRIM(customer_id)                             AS customer_id,
    TRIM(customer_name)                           AS customer_name,
    TRIM(segment)                                 AS segment,
    TRIM(city)                                    AS city,
    TRIM(state)                                   AS state,
    NULLIF(TRIM(postal_code), '')                 AS postal_code,
    TRIM(region)                                  AS region,
    TRIM(category)                                AS category,
    TRIM(sub_category)                            AS sub_category,
    TRIM(product_id)                              AS product_id,
    TRIM(product_name)                            AS product_name,
    CAST(quantity   AS UNSIGNED)                  AS quantity,
    CAST(discount   AS DECIMAL(5,2))              AS discount,
    CAST(unit_price AS DECIMAL(10,2))             AS unit_price,
    CAST(sales      AS DECIMAL(12,2))             AS sales,
    CAST(profit     AS DECIMAL(12,2))             AS profit
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY row_id        -- per row_id yang sama
            ORDER BY order_id          -- ambil salah satu saja
        ) AS rn
    FROM superstore_raw
    WHERE TRIM(order_id) != ''
      AND TRIM(sales)    != ''
) deduped
WHERE rn = 1;   -- hanya ambil 1 baris per row_id

-- Verifikasi hasilnya
SELECT COUNT(*) AS total_rows_staging FROM superstore_staging;

-- BAGIAN 4 : HAPUS DUPLIKAT

-- Lihat duplikat sebelum dihapus
SELECT
    order_id, product_id, order_date, customer_id, sales,
    COUNT(*) AS cnt
FROM superstore_staging
GROUP BY order_id, product_id, order_date, customer_id, sales
HAVING cnt > 1;

-- Hapus duplikat: simpan hanya row_id terkecil per kombinasi unik
DELETE s1
FROM superstore_staging s1
    INNER JOIN superstore_staging s2
        ON  s1.order_id     = s2.order_id
        AND s1.product_id   = s2.product_id
        AND s1.order_date   = s2.order_date
        AND s1.customer_id  = s2.customer_id
        AND s1.sales        = s2.sales
        AND s1.row_id       > s2.row_id;  

-- Konfirmasi
SELECT COUNT(*) AS rows_after_dedup FROM superstore_staging;



-- BAGIAN 5 : HANDLE NULL / MISSING VALUES

-- Lihat baris yang postal_code NULL
SELECT order_id, city, state, postal_code
FROM superstore_staging
WHERE postal_code IS NULL;

-- Isi postal_code NULL dengan 'UNKNOWN'
UPDATE superstore_staging
SET postal_code = 'UNKNOWN'
WHERE postal_code IS NULL;

-- Handle ship_date NULL (jika ada)
UPDATE superstore_staging
SET ship_date = DATE_ADD(order_date, INTERVAL 5 DAY)
WHERE ship_date IS NULL;

-- Verifikasi tidak ada NULL di kolom kritis
SELECT
    SUM(order_date IS NULL) AS null_order_date,
    SUM(ship_date IS NULL) AS null_ship_date,
    SUM(postal_code IS NULL) AS null_postal_code,
    SUM(sales IS NULL) AS null_sales,
    SUM(profit IS NULL) AS null_profit
FROM superstore_staging;


-- BAGIAN 6 : DETEKSI & TANGANI OUTLIER


-- 6a. Hitung batas outlier dengan IQR Method
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY profit) OVER() AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY profit) OVER() AS q3
    FROM superstore_staging
    LIMIT 1
)
SELECT
    q1, q3,
    (q3 - q1)            AS iqr,
    q1 - 1.5*(q3-q1)    AS lower_fence,
    q3 + 1.5*(q3-q1)    AS upper_fence
FROM stats
LIMIT 1;

-- 6b. Lihat transaksi dengan profit margin ekstrem (< -50%)
SELECT
    order_id, product_name, sub_category,
    quantity, unit_price, discount, sales, profit,
    ROUND(profit / NULLIF(sales, 0) * 100, 1) AS margin_pct
FROM superstore_staging
WHERE profit / NULLIF(sales, 0) < -0.5
ORDER BY profit / NULLIF(sales, 0) ASC;

-- 6c. Tambahkan flag untuk outlier (TIDAK dihapus, tapi di-flag)
ALTER TABLE superstore_staging
    ADD COLUMN is_outlier TINYINT DEFAULT 0;

UPDATE superstore_staging
SET is_outlier = 1
WHERE profit / NULLIF(sales, 0) < -0.5
   OR sales > 10000;   -- transaksi >$10K juga di-flag untuk review

SELECT
    SUM(is_outlier) AS flagged_outliers,
    COUNT(*)        AS total_rows
FROM superstore_staging;



-- BAGIAN 7 : VALIDASI AKHIR

-- 7a. Summary statistik akhir
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(DISTINCT order_id)                            AS unique_orders,
    COUNT(DISTINCT customer_id)                         AS unique_customers,
    MIN(order_date)                                     AS date_start,
    MAX(order_date)                                     AS date_end,
    ROUND(SUM(sales),   2)                              AS total_sales,
    ROUND(SUM(profit),  2)                              AS total_profit,
    ROUND(AVG(sales),   2)                              AS avg_order_value,
    ROUND(SUM(profit)/SUM(sales)*100, 2)               AS overall_margin_pct,
    SUM(is_outlier)                                     AS outlier_rows
FROM superstore_staging;

-- 7b. Distribusi per kategori utama
SELECT
    category,
    COUNT(*)                                            AS row_count,
    COUNT(DISTINCT order_id)                            AS orders,
    ROUND(SUM(sales), 0)                                AS total_sales,
    ROUND(SUM(profit), 0)                               AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)               AS margin_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY category
ORDER BY total_sales DESC;

-- 7c. Pastikan tidak ada tanggal aneh (ship sebelum order)
SELECT COUNT(*) AS negative_ship_duration
FROM superstore_staging
WHERE ship_date < order_date;


