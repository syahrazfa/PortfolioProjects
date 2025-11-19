-- preview
SELECT * FROM airbnbdata_staging LIMIT 10;

-- COLUMNS SPACE TO '_' and uppercase to lowercase
ALTER TABLE airbnbdata_staging
RENAME COLUMN "NAME" TO name;

ALTER TABLE airbnbdata_staging
RENAME "host id" TO host_id;

ALTER TABLE airbnbdata_staging
RENAME "neighbourhood group" TO neighbourhood_group;

ALTER TABLE airbnbdata_staging
RENAME "country code" TO country_code;

ALTER TABLE airbnbdata_staging
RENAME "host name" TO host_name;

ALTER TABLE airbnbdata_staging
RENAME "room type" TO room_type;

ALTER TABLE airbnbdata_staging
RENAME "Construction year" TO construction_year;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "service fee" TO service_fee;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "minimum nights" TO minimum_nights;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "number of reviews" TO number_of_reviews;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "last review" TO last_review;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "reviews per month" TO reviews_per_month;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "review rate number" TO review_rate_number;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "calculated host listings count" TO calculated_host_listings_count;

ALTER TABLE airbnbdata_staging
RENAME COLUMN "availability 365" TO availability_365;

-- 1. remove exact duplicate rows by id (keep one)
SELECT id, COUNT(*) AS duplicate_count
FROM airbnbdata_staging
GROUP BY id
HAVING COUNT(*) > 1;

DELETE FROM airbnbdata_staging a
USING airbnbdata_staging b
WHERE a.id = b.id
  AND a.ctid > b.ctid;  -- keep the first occurrence

-- 2. replace NULLs with simple defaults
UPDATE airbnbdata_staging
SET host_identity_verified = 'unconfirmed'
WHERE host_identity_verified IS NULL;

UPDATE airbnbdata_staging
SET country = 'unknown'
WHERE country IS NULL;

UPDATE airbnbdata_staging
SET country_code = 'US'
WHERE country_code IS NULL;

UPDATE airbnbdata_staging
SET cancellation_policy = 'missing'
WHERE cancellation_policy IS NULL;

UPDATE airbnbdata_staging
SET instant_bookable = 'UNKNOWN'
WHERE instant_bookable IS NULL;

UPDATE airbnbdata_staging
SET name = 'No Name Provided'
WHERE name IS NULL OR name = '';

UPDATE airbnbdata_staging
SET last_review = '1900-01-01'
WHERE last_review IS NULL;

UPDATE airbnbdata_staging
SET reviews_per_month = 0
WHERE reviews_per_month IS NULL;

UPDATE airbnbdata_staging
SET house_rules = 'No Rules Provided'
WHERE house_rules IS NULL;

-- 3. simple price/service_fee cleaning (remove $ and commas then cast)
UPDATE airbnbdata_staging
SET price = CAST(REPLACE(REPLACE(price::text, '$', ''), ',', '') AS numeric)
WHERE price IS NOT NULL AND price::text LIKE '%$%';

UPDATE airbnbdata_staging
SET service_fee = CAST(REPLACE(REPLACE(service_fee::text, '$', ''), ',', '') AS numeric)
WHERE service_fee IS NOT NULL AND service_fee::text LIKE '%$%';

-- if any price IS NULL, set to median value you already decided (simple)
UPDATE airbnbdata_staging
SET price = 475
WHERE price IS NULL;

UPDATE airbnbdata_staging
SET service_fee = 204
WHERE service_fee IS NULL;

-- 4. simple text normalization (lowercase)
UPDATE airbnbdata_staging
SET room_type = LOWER(room_type)
WHERE room_type IS NOT NULL;

UPDATE airbnbdata_staging
SET country = LOWER(country)
WHERE country IS NOT NULL;

UPDATE airbnbdata_staging
SET host_name = LOWER(host_name)
WHERE host_name IS NOT NULL;

UPDATE airbnbdata_staging
SET name = LOWER(name)
WHERE name IS NOT NULL;

-- 5. remove double spaces in name (basic)
UPDATE airbnbdata_staging
SET name = REPLACE(name, '  ', ' ')
WHERE name LIKE '%  %';

-- 6. quick checks after cleaning
SELECT COUNT(*) AS total_rows FROM airbnbdata_staging;

SELECT COUNT(*) FILTER (WHERE price IS NULL) AS price_nulls,
       COUNT(*) FILTER (WHERE name IS NULL OR name = '') AS name_missing
FROM airbnbdata_staging;

SELECT room_type, COUNT(*) FROM airbnbdata_staging
GROUP BY room_type
ORDER BY 2 DESC
LIMIT 50;

SELECT DISTINCT last_review FROM airbnbdata_staging LIMIT 50;

-- How many rows and missing price count
SELECT COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE price IS NULL) AS price_nulls
FROM airbnbdata_staging;

-- Top 20 highest prices to inspect outliers
SELECT id, name, price FROM airbnbdata_staging
ORDER BY price DESC;

CREATE TABLE airbnb_cleandata(
	LIKE airbnbdata_staging INCLUDING ALL
);

