-- ============================================================
--  SUPERSTORE SALES ANALYSIS
--  Step 04 : Advanced Analysis
-- ============================================================
--
--  ANALISIS LANJUTAN :
--  A. RFM Customer Segmentation
--  B. Cohort Retention Analysis
--  C. Running Total & Cumulative Growth
--  D. Rank produk per region (WINDOW FUNCTIONS)
--  E. Month-over-Month growth
--  F. Moving Average (smoothing trend)
-- ============================================================

USE superstore_db;

-- ============================================================
-- A. RFM CUSTOMER SEGMENTATION
--    Recency   : seberapa baru pembelian terakhir?
--    Frequency : seberapa sering beli?
--    Monetary  : seberapa besar total spend?
-- ============================================================

-- Tanggal referensi = 1 hari setelah tanggal order terakhir
SET @ref_date = (SELECT DATE_ADD(MAX(order_date), INTERVAL 1 DAY) FROM superstore_staging);

-- Step 1: hitung R, F, M per customer
DROP TEMPORARY TABLE IF EXISTS tmp_rfm_raw;
CREATE TEMPORARY TABLE tmp_rfm_raw AS
SELECT
    customer_id,
    customer_name,
    segment,
    region,
    DATEDIFF(@ref_date, MAX(order_date))        AS recency_days,
    COUNT(DISTINCT order_id)                    AS frequency,
    ROUND(SUM(sales), 2)                        AS monetary
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY customer_id, customer_name, segment, region;

-- Step 2: beri skor 1-5 dengan NTILE
DROP TEMPORARY TABLE IF EXISTS tmp_rfm_scored;
CREATE TEMPORARY TABLE tmp_rfm_scored AS
SELECT
    customer_id,
    customer_name,
    segment,
    region,
    recency_days,
    frequency,
    monetary,
    -- Recency: semakin kecil hari = lebih baik = skor tinggi
    NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
FROM tmp_rfm_raw;

-- Step 3: assign segment label
SELECT
    customer_id,
    customer_name,
    segment,
    region,
    recency_days,
    frequency,
    ROUND(monetary, 0)                          AS monetary,
    r_score, f_score, m_score,
    (r_score + f_score + m_score)              AS rfm_total,
    CASE
        WHEN (r_score + f_score + m_score) >= 13                        THEN 'Champion'
        WHEN (r_score + f_score + m_score) >= 10 AND r_score >= 3       THEN 'Loyal Customer'
        WHEN r_score >= 4 AND (f_score + m_score) >= 4                  THEN 'Potential Loyalist'
        WHEN r_score >= 4 AND f_score <= 2                              THEN 'New Customer'
        WHEN r_score <= 2 AND (f_score + m_score) >= 8                  THEN 'At Risk'
        WHEN r_score <= 2 AND (f_score + m_score) >= 5                  THEN 'Cant Lose Them'
        ELSE                                                              'Hibernating'
    END                                         AS rfm_segment
FROM tmp_rfm_scored
ORDER BY rfm_total DESC;

-- Summary RFM segment
SELECT
    rfm_segment,
    COUNT(*)                                    AS customer_count,
    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM tmp_rfm_scored), 1) AS pct_customer,
    ROUND(AVG(monetary), 0)                     AS avg_spend,
    ROUND(AVG(frequency), 1)                    AS avg_orders,
    ROUND(AVG(recency_days), 0)                AS avg_recency_days
FROM (
    SELECT *, (r_score + f_score + m_score) AS rfm_total,
        CASE
            WHEN (r_score + f_score + m_score) >= 13                        THEN 'Champion'
            WHEN (r_score + f_score + m_score) >= 10 AND r_score >= 3       THEN 'Loyal Customer'
            WHEN r_score >= 4 AND (f_score + m_score) >= 4                  THEN 'Potential Loyalist'
            WHEN r_score >= 4 AND f_score <= 2                              THEN 'New Customer'
            WHEN r_score <= 2 AND (f_score + m_score) >= 8                  THEN 'At Risk'
            WHEN r_score <= 2 AND (f_score + m_score) >= 5                  THEN 'Cant Lose Them'
            ELSE                                                              'Hibernating'
        END AS rfm_segment
    FROM tmp_rfm_scored
) rfm
GROUP BY rfm_segment
ORDER BY avg_spend DESC;


-- ============================================================
-- B. COHORT RETENTION ANALYSIS
--    Lihat berapa % customer yang kembali beli di bulan ke-N
--    setelah pertama kali beli
-- ============================================================

-- Bulan pertama tiap customer
WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_FORMAT(MIN(order_date), '%Y-%m') AS cohort_month
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY customer_id
),
-- Semua aktivitas tiap customer
activity AS (
    SELECT
        s.customer_id,
        fp.cohort_month,
        DATE_FORMAT(s.order_date, '%Y-%m')         AS activity_month,
        -- Hitung berapa bulan setelah cohort
        PERIOD_DIFF(
            DATE_FORMAT(s.order_date, '%Y%m'),
            DATE_FORMAT(STR_TO_DATE(CONCAT(fp.cohort_month, '-01'), '%Y-%m-%d'), '%Y%m')
        )                                           AS month_number
    FROM superstore_staging s
        JOIN first_purchase fp ON s.customer_id = fp.customer_id
    WHERE s.is_outlier = 0
)
SELECT
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) AS active_customers
FROM activity
WHERE month_number BETWEEN 0 AND 11   -- 12 bulan retention
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;


-- ============================================================
-- C. RUNNING TOTAL & CUMULATIVE GROWTH
-- ============================================================

SELECT
    DATE_FORMAT(order_date, '%Y-%m')            AS month,
    ROUND(SUM(sales), 0)                        AS monthly_revenue,
    ROUND(SUM(SUM(sales)) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS UNBOUNDED PRECEDING
    ), 0)                                       AS ytd_revenue,
    ROUND(SUM(profit), 0)                       AS monthly_profit,
    ROUND(SUM(SUM(profit)) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS UNBOUNDED PRECEDING
    ), 0)                                       AS ytd_profit
FROM superstore_staging
WHERE is_outlier = 0
GROUP BY order_year, order_month, month
ORDER BY month;


-- ============================================================
-- D. RANK PRODUK PER REGION (WINDOW FUNCTION)
--    Top 3 produk terlaris di setiap region
-- ============================================================

WITH product_region AS (
    SELECT
        region,
        sub_category,
        ROUND(SUM(sales), 0)    AS revenue,
        ROUND(SUM(profit), 0)   AS profit,
        ROUND(SUM(profit)/SUM(sales)*100, 2) AS margin_pct,
        DENSE_RANK() OVER (
            PARTITION BY region
            ORDER BY SUM(sales) DESC
        )                        AS rank_in_region
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY region, sub_category
)
SELECT *
FROM product_region
WHERE rank_in_region <= 3
ORDER BY region, rank_in_region;


-- ============================================================
-- E. MONTH-OVER-MONTH (MoM) GROWTH
-- ============================================================

WITH monthly AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m')    AS month,
        order_year, order_month,
        ROUND(SUM(sales), 0)               AS revenue
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY month, order_year, order_month
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month)     AS prev_month_revenue,
    revenue - LAG(revenue) OVER (ORDER BY month) AS mom_delta,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100
    , 1)                                   AS mom_growth_pct
FROM monthly
ORDER BY month;


-- ============================================================
-- F. 3-MONTH MOVING AVERAGE (SMOOTHING TREND)
-- ============================================================

WITH monthly_rev AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        ROUND(SUM(sales), 0)            AS revenue
    FROM superstore_staging
    WHERE is_outlier = 0
    GROUP BY month
)
SELECT
    month,
    revenue,
    ROUND(AVG(revenue) OVER (
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0)                               AS moving_avg_3m,
    ROUND(AVG(revenue) OVER (
        ORDER BY month
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ), 0)                               AS moving_avg_6m
FROM monthly_rev
ORDER BY month;

