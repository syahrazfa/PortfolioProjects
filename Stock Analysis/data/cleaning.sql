-- DATA CLEANING

SELECT ticker, COUNT(*) 
FROM stock.stockdata_staging
GROUP BY ticker;

-- DUPLICATION CHECK AND DELETE IF EXIST
---------------------------------------------------------------

SELECT ticker, date, count(*)
FROM stock.stockdata_staging
GROUP BY ticker,date
HAVING COUNT(*) > 1;

DELETE FROM stock.stockdata_staging a
USING stock.stockdata_staging b
WHERE a.ctid < b.ctid
AND a.ticker = b.ticker
AND a.date = b.date;

-- DATE
---------------------------------------------------------------
SELECT *
FROM stock.stock_raw
WHERE date IS NULL
   OR date < '1980-01-01'
   OR date > CURRENT_DATE;

-- MISSING VALUE
---------------------------------------------------------------
SELECT *
FROM stock.stock_raw
WHERE open IS NULL
   OR high IS NULL
   OR low IS NULL
   OR close IS NULL;

-- ZERO VALUE
---------------------------------------------------------------
SELECT *
FROM stock.stock_raw
WHERE close <= 0
   OR open <= 0
   OR high <= 0
   OR low <= 0;

-- VOLUME
---------------------------------------------------------------
SELECT *
FROM stock.stock_raw
WHERE volume IS NULL
   OR volume < 0;

-- PRICE VALIDATION RELATIONSHIP
---------------------------------------------------------------
SELECT *
FROM stock.stock_raw
WHERE low > open
   OR low > close
   OR open > high
   OR close > high;

-- TICKER DATE CHECKER
---------------------------------------------------------------
   
SELECT ticker, MIN(date), MAX(date), COUNT(*)
FROM stock.stock_raw
GROUP BY ticker
ORDER BY ticker;


