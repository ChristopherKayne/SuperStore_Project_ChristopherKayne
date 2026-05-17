--  PERTANYAAN BISNIS YANG DIJAWAB :
--  Q1.  Bagaimana trend revenue & profit dari tahun ke tahun?
--  Q2.  Kategori & sub-kategori mana yang paling & paling tidak profitable?
--  Q3.  Apakah diskon berdampak negatif pada profitabilitas?
--  Q4.  Segment mana yang paling valuable?
--  Q5.  Region mana yang perlu perhatian?
--  Q6.  Produk apa yang harus di-discontinue?
--  Q7.  Bagaimana pola musiman penjualan?
-- ============================================================

USE superstore_db;

-- Gunakan data yang sudah bersih (tanpa outlier)
-- atau kalau mau include outlier: hapus kondisi WHERE is_outlier = 0

-- ============================================================
-- Q1. TREND REVENUE & PROFIT (YoY)
-- ============================================================

-- Tahunan
SELECT
    order_year                              AS tahun,
    COUNT(DISTINCT order_id)               AS total_orders,
    COUNT(DISTINCT customer_id)            AS unique_customers,
    ROUND(SUM(sales), 0)                   AS revenue,
    ROUND(SUM(profit), 0)                  AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)  AS margin_pct,
    ROUND(SUM(sales)/COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY order_year
ORDER BY tahun;

-- YoY Growth (pakai LAG window function)
WITH yearly AS (
    SELECT
        order_year,
        SUM(sales)  AS revenue,
        SUM(profit) AS profit
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY order_year
)
SELECT
    order_year,
    ROUND(revenue, 0)                                                          AS revenue,
    ROUND(profit, 0)                                                           AS profit,
    LAG(revenue) OVER (ORDER BY order_year)                                    AS prev_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY order_year))
          / LAG(revenue) OVER (ORDER BY order_year) * 100, 1)                 AS revenue_growth_pct,
    ROUND((profit - LAG(profit) OVER (ORDER BY order_year))
          / ABS(LAG(profit) OVER (ORDER BY order_year)) * 100, 1)             AS profit_growth_pct
FROM yearly;

-- Bulanan (untuk chart Power BI)
SELECT
    order_year,
    order_month,
    DATE_FORMAT(order_date, '%Y-%m') AS year_months,
    ROUND(SUM(sales),  0)             AS revenue,
    ROUND(SUM(profit), 0)             AS profit,
    COUNT(DISTINCT order_id)          AS orders
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY order_year, order_month, year_months
ORDER BY year_months;


-- ============================================================
-- Q2. PROFITABILITAS PER KATEGORI & SUB-KATEGORI
-- ============================================================

-- Per kategori
SELECT
    category,
    SUM(quantity)                                      AS units_sold,
    ROUND(SUM(sales), 0)                               AS revenue,
    ROUND(SUM(profit), 0)                              AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)              AS margin_pct,
    ROUND(SUM(sales)/SUM(SUM(sales)) OVER()*100, 1)  AS revenue_share_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY category
ORDER BY profit DESC;

-- Per sub-kategori (detail)
SELECT
    category,
    sub_category,
    COUNT(DISTINCT order_id)                           AS orders,
    SUM(quantity)                                      AS units_sold,
    ROUND(SUM(sales), 0)                               AS revenue,
    ROUND(SUM(profit), 0)                              AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)              AS margin_pct,
    ROUND(AVG(unit_price), 2)                          AS avg_unit_price,
    ROUND(AVG(discount)*100, 1)                        AS avg_discount_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY category, sub_category
ORDER BY margin_pct DESC;

-- TOP 5 sub-kategori paling rugi
SELECT
    sub_category,
    ROUND(SUM(profit), 0)                              AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)              AS margin_pct,
    COUNT(DISTINCT order_id)                           AS orders,
    ROUND(AVG(discount)*100, 1)                        AS avg_discount_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY sub_category
ORDER BY total_profit ASC
LIMIT 5;


-- ============================================================
-- Q3. DAMPAK DISKON TERHADAP PROFITABILITAS
-- ============================================================

-- Kelompokkan berdasarkan tier diskon
SELECT
    CASE
        WHEN discount = 0.00             THEN '0%   - No Discount'
        WHEN discount BETWEEN 0.01 AND 0.10 THEN '1-10%  - Low'
        WHEN discount BETWEEN 0.11 AND 0.20 THEN '11-20% - Moderate'
        WHEN discount BETWEEN 0.21 AND 0.30 THEN '21-30% - High'
        WHEN discount BETWEEN 0.31 AND 0.40 THEN '31-40% - Very High'
        ELSE                                   '40%+   - Extreme'
    END                                            AS discount_tier,
    COUNT(*)                                       AS transactions,
    ROUND(SUM(sales), 0)                           AS total_revenue,
    ROUND(SUM(profit), 0)                          AS total_profit,
    ROUND(AVG(profit), 2)                          AS avg_profit_per_txn,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS margin_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY discount_tier
ORDER BY discount_tier;

-- Correlation proxy: discount vs margin per sub-category
SELECT
    sub_category,
    ROUND(AVG(discount)*100, 1)                    AS avg_discount_pct,
    ROUND(AVG(profit/NULLIF(sales,0))*100, 2)      AS avg_margin_pct,
    COUNT(*)                                       AS txn_count
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY sub_category
ORDER BY avg_discount_pct DESC;

-- Berapa kerugian total akibat diskon tinggi (>30%)?
SELECT
    COUNT(*)                                       AS txn_rugi,
    ROUND(SUM(profit), 0)                          AS total_loss,
    ROUND(SUM(sales), 0)                           AS revenue_terkait,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS effective_margin
FROM superstore_staging
WHERE discount >= 0.30
  AND profit < 0
  AND is_outlier = 0;


-- ============================================================
-- Q4. ANALISIS SEGMEN PELANGGAN
-- ============================================================

SELECT
    segment,
    COUNT(DISTINCT customer_id)                    AS unique_customers,
    COUNT(DISTINCT order_id)                       AS total_orders,
    ROUND(SUM(sales), 0)                           AS revenue,
    ROUND(SUM(profit), 0)                          AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS margin_pct,
    ROUND(SUM(sales)/COUNT(DISTINCT order_id), 2) AS avg_order_value,
    ROUND(SUM(sales)/COUNT(DISTINCT customer_id), 2) AS revenue_per_customer,
    ROUND(COUNT(DISTINCT order_id)/COUNT(DISTINCT customer_id), 2) AS orders_per_customer
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY segment
ORDER BY revenue DESC;

-- Trend segment per tahun
SELECT
    order_year,
    segment,
    ROUND(SUM(sales), 0)                           AS revenue,
    ROUND(SUM(profit), 0)                          AS profit,
    COUNT(DISTINCT customer_id)                    AS customers
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY order_year, segment
ORDER BY order_year, revenue DESC;


-- ============================================================
-- Q5. ANALISIS REGIONAL
-- ============================================================

SELECT
    region,
    state,
    COUNT(DISTINCT customer_id)                    AS customers,
    COUNT(DISTINCT order_id)                       AS orders,
    ROUND(SUM(sales), 0)                           AS revenue,
    ROUND(SUM(profit), 0)                          AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS margin_pct,
    ROUND(AVG(ship_duration), 1)                   AS avg_ship_days
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY region, state
ORDER BY region, revenue DESC;

-- Top & Bottom 5 states by profit
(SELECT state, ROUND(SUM(profit),0) AS profit, 'TOP 5' AS label
 FROM superstore_staging WHERE is_outlier = 0
 GROUP BY state ORDER BY profit DESC LIMIT 5)
UNION ALL
(SELECT state, ROUND(SUM(profit),0) AS profit, 'BOTTOM 5' AS label
 FROM superstore_staging WHERE is_outlier = 0
 GROUP BY state ORDER BY profit ASC LIMIT 5)
ORDER BY profit DESC;


-- ============================================================
-- Q6. PRODUK YANG HARUS DI-REVIEW / DISCONTINUE
-- ============================================================

-- Produk paling profitable
SELECT
    product_name, category, sub_category,
    COUNT(DISTINCT order_id)                       AS orders,
    SUM(quantity)                                  AS units_sold,
    ROUND(SUM(sales), 0)                           AS revenue,
    ROUND(SUM(profit), 0)                          AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS margin_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY product_name, category, sub_category
ORDER BY profit DESC
LIMIT 15;

-- Produk paling merugi (kandidat discontinue)
SELECT
    product_name, category, sub_category,
    COUNT(DISTINCT order_id)                       AS orders,
    SUM(quantity)                                  AS units_sold,
    ROUND(SUM(sales), 0)                           AS revenue,
    ROUND(SUM(profit), 0)                          AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)          AS margin_pct,
    ROUND(AVG(discount)*100, 1)                    AS avg_disc_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY product_name, category, sub_category
HAVING orders >= 3     -- minimal 3 order agar relevan
ORDER BY profit ASC
LIMIT 15;


-- ============================================================
-- Q7. POLA MUSIMAN
-- ============================================================

-- Per kuartal
SELECT
    order_year,
    order_quarter,
    CONCAT('Q', order_quarter, '-', order_year) AS period,
    COUNT(DISTINCT order_id)                   AS orders,
    ROUND(SUM(sales), 0)                       AS revenue,
    ROUND(SUM(profit), 0)                      AS profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2)      AS margin_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY order_year, order_quarter
ORDER BY order_year, order_quarter;

-- Rata-rata revenue per bulan (aggregated lintas tahun)
SELECT
    order_month                                AS bulan,
    CASE order_month
        WHEN 1  THEN 'Jan' WHEN 2  THEN 'Feb' WHEN 3  THEN 'Mar'
        WHEN 4  THEN 'Apr' WHEN 5  THEN 'May' WHEN 6  THEN 'Jun'
        WHEN 7  THEN 'Jul' WHEN 8  THEN 'Aug' WHEN 9  THEN 'Sep'
        WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
    END                                        AS nama_bulan,
    COUNT(DISTINCT order_id)                   AS avg_orders,
    ROUND(AVG(monthly_rev), 0)                 AS avg_revenue
FROM (
    SELECT
        order_month,
        order_year,
        SUM(sales)                             AS monthly_rev,
        COUNT(DISTINCT order_id)               AS orders
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY order_month, order_year
) monthly
GROUP BY order_month
ORDER BY bulan;

-- ============================================================
-- BONUS : Shipping analysis
-- ============================================================

SELECT
    ship_mode,
    COUNT(*) AS orders,
    ROUND(AVG(ship_duration), 1) AS avg_days,
    MIN(ship_duration)           AS min_days,
    MAX(ship_duration)           AS max_days,
    ROUND(SUM(sales), 0)         AS revenue,
    ROUND(SUM(profit)/SUM(sales)*100, 2) AS margin_pct
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY ship_mode
ORDER BY avg_days ASC;

SELECT 'EDA selesai! Lanjut ke 04_advanced_analysis.sql' AS status;
