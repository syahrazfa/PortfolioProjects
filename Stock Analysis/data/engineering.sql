-- MAKING A BASE TABLE

DROP TABLE IF EXISTS stock.stock_daily_clean;
CREATE TABLE stock.stock_daily_clean AS
SELECT
  date::date        AS date,
  ticker            AS ticker,
  open::numeric     AS open,
  high::numeric     AS high,
  low::numeric      AS low,
  close::numeric    AS close,
  volume::numeric   AS volume
FROM stock.stockdata_staging;

ALTER TABLE stock.stock_daily_clean ADD COLUMN ingest_ts timestamptz DEFAULT now();
CREATE INDEX idx_daily_ticker_date ON stock.stock_daily_clean(ticker, date);


-- DAILY RETURN  
------------------------------------------------------------------------------------
CREATE TABLE stock.daily_return AS
SELECT 
	date,
	ticker,
	close,
	LAG(close) OVER(PARTITION BY ticker ORDER BY date) AS prev_close,
	(close - LAG(close) OVER(PARTITION BY ticker ORDER by date)) / NULLIF(LAG(close) OVER(PARTITION BY ticker ORDER BY date), 0) AS daily_return
FROM stock.stock_daily_clean;

CREATE INDEX idx_daily_returns_ticker_date ON stock.daily_return(ticker, date);

select * from stock.daily_return

--- MONTHLY RETURN
------------------------------------------------------------------------------------

-- 1. CREATE MONTHLY PRICE TABLE

CREATE TABLE stock.monthly_prices AS
SELECT
	ticker,
	date_trunc('month', date) AS month,
	MAX(date) AS last_trade_date
FROM stock.stock_daily_clean
GROUP BY ticker,date_trunc('month', date);

-- 2. MONTHLY-END CLOSE PRICE

CREATE TABLE stock.monthly_prices_with_close AS
SELECT
  m.ticker,
  m.month,
  m.last_trade_date,
  s.close AS month_end_price
FROM stock.monthly_prices m
JOIN stock.stock_daily_clean s
  ON s.ticker = m.ticker
  AND s.date = m.last_trade_date;

-- 3. ADD PREVIOUS MONTH USING LAG

CREATE TABLE stock.monthly_returns AS
SELECT
	month,
	ticker,
	month_end_price,
	LAG(month_end_price) OVER (PARTITION BY ticker ORDER BY MONTH) AS prev_month_price
FROM stock.monthly_prices_with_close;

-- 4. COMPUTE RETURN

ALTER TABLE stock.monthly_returns
ADD COLUMN monthly_return numeric;

UPDATE stock.monthly_returns
SET monthly_return = (monthly_returns.month_end_price - monthly_returns.prev_month_price) / NULLIF(monthly_returns.prev_month_price, 0)
	
SELECT * FROM stock.monthly_returns	

-- MONTHLY RETURN VALIDATION 
------------------------------------------------------------------------------------

-- 1. CHECK WHETHER THE FIRST MONTH OF EACH TICKER IS NULL

WITH first_month AS(
	SELECT 
		ticker,
		month,
		monthly_return,
		ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY month ASC) AS rn
	FROM stock.monthly_returns
)

SELECT ticker, month, monthly_return
FROM first_month
WHERE rn = 1;

-- 2. Calculate Volatility (Risk)

-- GETTING THE AVERAGE MONHTLY RETURN

SELECT 
    ticker,
    ROUND(AVG(monthly_return) * 100, 2) AS avg_monthly_return
FROM stock.monthly_returns
GROUP BY ticker
ORDER BY ticker;

-- GETTING THE RISK USING STANDARD DEVIATION

SELECT 
    ticker,
    ROUND(STDDEV(monthly_return) * 100, 2) AS volatility
FROM stock.monthly_returns
GROUP BY ticker
ORDER BY volatility DESC;

-- 3. SUMMARY TABLE // ADD AVERAGE MONTHLY RETURN, VOLATILITY

CREATE TABLE stock.performance_summary_table AS 
SELECT
    ticker,

    -- average monthly return (converted to % with 4 decimals)
    ROUND(AVG(monthly_return) * 100, 2) AS avg_monthly_return,

    -- volatility (stddev) in %
    ROUND(STDDEV(monthly_return) * 100, 2) AS volatility,

    -- best and worst month in %
    ROUND(MAX(monthly_return) * 100, 2) AS best_month,
    ROUND(MIN(monthly_return) * 100, 2) AS worst_month,

    -- count of positive and negative months
    COUNT(*) FILTER (WHERE monthly_return > 0) AS positive_months,
    COUNT(*) FILTER (WHERE monthly_return < 0) AS negative_months,

    -- total months (excluding NULLs automatically)
    COUNT(monthly_return) AS total_months

FROM stock.monthly_returns
GROUP BY ticker
ORDER BY ticker;

SELECT * FROM stock.performance_summary_table;

-- 4. STOCKS CORRELATION

SELECT
  a.month::date AS month_date,
  a.ticker AS ticker_a,
  ROUND(a.monthly_return * 100, 2) AS return_a,
  b.ticker AS ticker_b,
  ROUND(b.monthly_return * 100, 2) AS return_b
FROM stock.monthly_returns a
JOIN stock.monthly_returns b
  ON a.month = b.month          
  AND a.ticker = 'AAPL'        
  AND b.ticker = 'MSFT'        
ORDER BY a.month;

-- Compute the correlation for the pair

SELECT
    ROUND(CORR(a.monthly_return, b.monthly_return)::numeric, 4) AS correlation
FROM stock.monthly_returns a
JOIN stock.monthly_returns b
  ON a.month = b.month
WHERE a.ticker = 'AAPL'
  AND b.ticker = 'MSFT';

-- CORRELATION TABLE

DROP TABLE IF EXISTS stock.correlation_pairs;

CREATE TABLE stock.correlation_pairs AS
SELECT
    a.ticker AS ticker_a,
    b.ticker AS ticker_b,
    ROUND(CORR(a.monthly_return, b.monthly_return)::numeric, 4) AS correlation,
    COUNT(*) AS n_months,

    CASE
        WHEN CORR(a.monthly_return, b.monthly_return) >= 0.8 THEN 'Very Strong'
        WHEN CORR(a.monthly_return, b.monthly_return) >= 0.6 THEN 'Strong'
        WHEN CORR(a.monthly_return, b.monthly_return) >= 0.4 THEN 'Moderate'
        WHEN CORR(a.monthly_return, b.monthly_return) >= 0.2 THEN 'Weak'
        WHEN CORR(a.monthly_return, b.monthly_return) >= 0.0 THEN 'Very Weak'
        ELSE 'Negative'
    END AS correlation_scale

FROM stock.monthly_returns a
JOIN stock.monthly_returns b
    ON a.month = b.month
   AND a.ticker < b.ticker

GROUP BY a.ticker, b.ticker
ORDER BY ABS(CORR(a.monthly_return, b.monthly_return)) DESC;

SELECT * FROM stock.correlation_pairs;

-- CORRELATION MATRIX

WITH tickers AS (
  -- list all tickers present in pairs (either side)
  SELECT DISTINCT ticker_a AS ticker FROM stock.correlation_pairs
  UNION
  SELECT DISTINCT ticker_b AS ticker FROM stock.correlation_pairs
),
all_pairs AS (
  -- keep existing pairs
  SELECT ticker_a, ticker_b, correlation FROM stock.correlation_pairs
  UNION ALL
  -- add swapped pairs so matrix is symmetric
  SELECT ticker_b AS ticker_a, ticker_a AS ticker_b, correlation FROM stock.correlation_pairs
  UNION ALL
  -- add self-correlation rows
  SELECT t.ticker AS ticker_a, t.ticker AS ticker_b, 1.0 AS correlation
  FROM tickers t
)

SELECT
  ticker_a AS ticker,
  ROUND((MAX(correlation) FILTER (WHERE ticker_b = 'AAPL'))::numeric, 2) AS corr_with_AAPL,
  ROUND((MAX(correlation) FILTER (WHERE ticker_b = 'AMZN'))::numeric, 2) AS corr_with_AMZN,
  ROUND((MAX(correlation) FILTER (WHERE ticker_b = 'MSFT'))::numeric, 2) AS corr_with_MSFT,
  ROUND((MAX(correlation) FILTER (WHERE ticker_b = 'NVDA'))::numeric, 2) AS corr_with_NVDA,
  ROUND((MAX(correlation) FILTER (WHERE ticker_b = 'TSLA'))::numeric, 2) AS corr_with_TSLA
FROM all_pairs
GROUP BY ticker_a
ORDER BY ticker_a;

-- Drawdown Table
------------------------------------------------------------------------------------

-- 1. CUMULATIVE RETURN PER TICKER

DROP TABLE IF EXISTS stock.cum_index;

CREATE TABLE stock.cum_index AS
SELECT
    ticker,
    month,
    (1 + monthly_return) *
    EXP(SUM(LN(1 + monthly_return)) OVER
       (PARTITION BY ticker ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) 
       AS cum_index
FROM stock.monthly_returns;

SELECT * FROM stock.cum_index

-- 2. PEAK AND DRAWDOWN
DROP TABLE IF EXISTS stock.drawdown;

CREATE TABLE stock.drawdown AS
SELECT
    ticker,
    month,
    cum_index,
    MAX(cum_index) OVER (PARTITION BY ticker ORDER BY month) AS peak,
    (cum_index / MAX(cum_index) OVER (PARTITION BY ticker ORDER BY month) - 1) AS drawdown
FROM stock.cum_index;


SELECT * FROM stock.drawdown;

-- 3. MAX DRAWDOWN

DROP TABLE IF EXISTS stock.summary_table;

CREATE TABLE stock.summary_table AS
SELECT
    p.ticker,

    -- average monthly return in percent (2 decimals)
    ROUND(AVG(m.monthly_return) * 100::numeric, 2) AS avg_return_pct,

    -- volatility (stddev) in percent
    ROUND(STDDEV(m.monthly_return) * 100::numeric, 2) AS volatility_pct,

    -- best and worst single-month returns in percent
    ROUND(MAX(m.monthly_return) * 100::numeric, 2) AS best_month_pct,
    ROUND(MIN(m.monthly_return) * 100::numeric, 2) AS worst_month_pct,

    -- counts from performance summary table (already computed)
    p.positive_months,
    p.negative_months,

    -- max drawdown already stored as percent, keep it (ensure it's numeric and rounded)
    ROUND(md.max_drawdown::numeric, 2) AS max_drawdown_pct,

    -- sharpe ratio (unitless)
    ROUND(
        (AVG(m.monthly_return) / NULLIF(STDDEV(m.monthly_return), 0))::numeric,
        4
    ) AS sharpe_ratio

FROM stock.monthly_returns m
JOIN stock.performance_summary_table p
  ON m.ticker = p.ticker
JOIN stock.max_drawdown md
  ON m.ticker = md.ticker

GROUP BY
    p.ticker,
    p.positive_months,
    p.negative_months,
    md.max_drawdown
ORDER BY sharpe_ratio DESC;

SELECT * FROM stock.summary_table;
