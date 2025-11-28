-- MANUALLY COPY CSV TO DATABASE

-- TRUNCATE stock.staging_stock;

-- copy stock.staging_stock FROM 'D:/Workspace/SQL/PortfolioProject/Stock Analysis/data/stocks/NVDA_us_d.csv' CSV HEADER;

-- INSERT INTO stock.stock_raw(date, open, high, low, close, volume, ticker)
-- SELECT date::date, open::numeric, high::numeric, low::numeric, close::numeric, volume::numeric, 'NVDA'
-- FROM stock.staging_stock;

SELECT * FROM stock.stock_raw;