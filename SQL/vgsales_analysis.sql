-- ========================================
-- Project: Video Game Sales Analysis
-- Dataset: Kaggle - Video Game Sales
-- Database: MySQL
-- ========================================


SELECT * FROM vgsales;


-- Create Staging Tables
CREATE TABLE vgsales_stagging
LIKE vgsales;

SELECT * 
FROM vgsales_stagging;

INSERT INTO vgsales_stagging
SELECT *
FROM vgsales;


-- 1. Remove Duplicates Records

SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY `Name`, Platform, `Year`, Genre, Publisher, NA_sales, EU_sales, JP_sales, Other_sales, Global_sales)  AS row_num
FROM vgsales_stagging;

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY `Name`, Platform, `Year`, Genre, Publisher, NA_sales, EU_sales, JP_sales, Other_sales, Global_sales)  AS row_num
FROM vgsales_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

SELECT * 
FROM vgsales_stagging
WHERE `Name` LIKE '%Wii de Asobu: Metroid Prime%';

CREATE TABLE `vgsales_stagging2` (
  `Rank` int DEFAULT NULL,
  `Name` text,
  `Platform` text,
  `Year` text,
  `Genre` text,
  `Publisher` text,
  `NA_Sales` double DEFAULT NULL,
  `EU_Sales` double DEFAULT NULL,
  `JP_Sales` double DEFAULT NULL,
  `Other_Sales` double DEFAULT NULL,
  `Global_Sales` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM vgsales_stagging2;

INSERT INTO vgsales_stagging2
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY `Name`, Platform, `Year`, Genre, Publisher, NA_sales, EU_sales, JP_sales, Other_sales, Global_sales)  AS row_num
FROM vgsales_stagging;

DELETE
FROM vgsales_stagging2
WHERE row_num>1;

-- 2. Standardizing data

SELECT *
FROM vgsales_stagging2;

SELECT `Name`, COUNT(*) AS total
FROM vgsales_stagging2
GROUP BY `Name`
ORDER BY total DESC;

UPDATE vgsales_stagging2
SET 
	`Name` = LOWER(TRIM(`Name`)),
	Platform = LOWER(TRIM(Platform)),
	Genre = LOWER(TRIM(Genre)),
	Publisher = LOWER(TRIM(Publisher));

SELECT COUNT(*) AS invalid_year_count
FROM vgsales_stagging2
WHERE year REGEXP '[^0-9]'
   OR year IS NULL
   OR year = '';

-- Clean invalid year values
UPDATE vgsales_stagging2
SET `Year` = NULL
WHERE `Year` REGEXP '[^0-9]'
   OR `Year` = '';

SELECT *
FROM vgsales_stagging2
WHERE `year` REGEXP '[^0-9]';

SELECT *
FROM vgsales_stagging2;

-- 3. Null Value or blank values
SELECT COUNT(*) AS invalid_year_count
FROM vgsales_stagging2
WHERE year REGEXP '[^0-9]'
   OR year IS NULL
   OR year = '';

UPDATE vgsales_stagging2
SET `Year` = NULL
WHERE `Year` REGEXP '[^0-9]'
   OR `Year` = '';

SELECT DISTINCT year
FROM vgsales_stagging2
ORDER BY year;

ALTER TABLE vgsales_stagging2
MODIFY year INT;


-- 4. Removes Any Columns. 
ALTER TABLE vgsales_stagging2
DROP COLUMN row_num;

SELECT *
FROM vgsales_stagging2;

-- Games released per year
SELECT `year`,
	COUNT(*) AS frequency,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Year IS NOT NULL
GROUP BY Year
ORDER BY persentase DESC
LIMIT 5;

-- Global sales statistics
SELECT
    AVG(NA_Sales) AS avg_NA,
    AVG(EU_Sales) AS avg_EU,
    AVG(JP_Sales) AS avg_JP,
    AVG(Other_Sales) AS avg_Other,
    AVG(Global_Sales) AS avg_Global,
    MIN(Global_Sales) AS min_Global,
    MAX(Global_Sales) AS max_Global,
	stddev(Global_Sales) AS std_Global
FROM vgsales_stagging2;

-- Percentage Contribution Regional
SELECT 
    Name,
    NA_Sales / Global_Sales * 100 AS pct_NA,
    EU_Sales / Global_Sales * 100 AS pct_EU,
    JP_Sales / Global_Sales * 100 AS pct_JP,
    Other_Sales / Global_Sales * 100 AS pct_Other
FROM vgsales_stagging2;

-- Aggregation Sales per Year
SELECT Year, 
       SUM(NA_Sales) AS total_NA,
       SUM(EU_Sales) AS total_EU,
       SUM(JP_Sales) AS total_JP,
       SUM(Other_Sales) AS total_Other,
       SUM(Global_Sales) AS total_Global
FROM vgsales_stagging2
WHERE Year IS NOT NULL
GROUP BY Year
ORDER BY Year;


-- Top Games by Sales
SELECT `Name`,
	COUNT(*) AS frequency,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Year IS NOT NULL
GROUP BY Name
ORDER BY persentase DESC
LIMIT 5;

-- Top platforms by number of games
SELECT Platform,
	COUNT(*) AS frequency,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Platform IS NOT NULL
GROUP BY Platform
ORDER BY persentase DESC
LIMIT 5;

-- Top Genre by number of Genres
SELECT Genre,
	COUNT(*) AS frequency,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Genre IS NOT NULL
GROUP BY Genre
ORDER BY persentase DESC
LIMIT 5;

-- Top Publisher
SELECT Publisher,
	COUNT(*) AS frequency,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Publisher IS NOT NULL
GROUP BY Publisher
ORDER BY persentase DESC
LIMIT 5;

-- Top Games per Region
SELECT `Name`,Platform, NA_Sales
FROM vgsales_stagging2
ORDER BY NA_Sales DESC
LIMIT 5;

SELECT `Name`,Platform, EU_Sales
FROM vgsales_stagging2
ORDER BY NA_Sales DESC
LIMIT 5;

SELECT `Name`,Platform, JP_Sales
FROM vgsales_stagging2
ORDER BY NA_Sales DESC
LIMIT 5;

SELECT `Name`,Platform, Other_Sales
FROM vgsales_stagging2
ORDER BY NA_Sales DESC
LIMIT 5;


-- Top Global Sales (1980-2016)
SELECT Name, Platform, Year, Global_Sales,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS persentase
FROM vgsales_stagging2
WHERE Year BETWEEN 1980 AND 2016
GROUP BY Name, Platform, Year, Global_Sales
ORDER BY Global_Sales DESC
LIMIT 10;

WITH top10 AS (
    SELECT Name
    FROM vgsales
    WHERE Year BETWEEN 1980 AND 2016
    ORDER BY Global_Sales DESC
    LIMIT 10
)
SELECT Name, COUNT(*) AS freq
FROM vgsales
WHERE Name IN (SELECT Name FROM top10)
GROUP BY Name
ORDER BY freq DESC;

