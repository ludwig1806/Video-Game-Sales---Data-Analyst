-- Create Staging Table (raw import)

CREATE TABLE housing_stage2 (
    UniqueID VARCHAR(50),
    ParcelID VARCHAR(50),
    LandUse VARCHAR(50),
    PropertyAddress VARCHAR(255),
    SaleDate VARCHAR(50),
    SalePrice VARCHAR(50),
    LegalReference VARCHAR(50),
    SoldAsVacant VARCHAR(10),
    OwnerName VARCHAR(255),
    OwnerAddress VARCHAR(255),
    Acreage VARCHAR(50),
    TaxDistrict VARCHAR(100),
    LandValue VARCHAR(50),
    BuildingValue VARCHAR(50),
    TotalValue VARCHAR(50),
    YearBuilt VARCHAR(50),
    Bedrooms VARCHAR(50),
    FullBath VARCHAR(50),
    HalfBath VARCHAR(50)
);


/*

Cleaning Data in SQL Queries

*/

SELECT 
    *
FROM
    housing_stage;

--------------------------------------------------------------------------
-- Create New Table Stagging

CREATE TABLE housing_stage2 LIKE housing_stage;

SELECT 
    *
FROM
    housing_stage;

INSERT INTO housing_stage2
SELECT *
FROM housing_stage;

SELECT 
    *
FROM
    housing_stage2;

--------------------------------------------------------------------------

-- Standardize Date Format

SELECT 
    SaleDate, STR_TO_DATE(SaleDate, '%M %d, %Y')
FROM
    housing_stage2;

ALTER TABLE housing_stage2
ADD SaleDateConverted Date;

UPDATE housing_stage2 
SET 
    SaleDateConverted = STR_TO_DATE(SaleDate, '%M %d, %Y');

SELECT 
    SaleDateConverted
FROM
    housing_stage2;


--------------------------------------------------------------------------

-- Populate Property Address Data
SELECT * , COUNT(*) OVER (partition by PropertyAddress ) suspicious_rows
FROM housing_stage2
WHERE PropertyAddress = '' OR PropertyAddress IS NULL;

SELECT 
    *
FROM
    housing_stage2
WHERE
    ParcelID = '034 03 0 059.00';

SELECT 
    a.UniqueID,
    a.ParcelID,
    a.PropertyAddress,
    b.UniqueID,
    b.ParcelID,
    b.PropertyAddress,
    COALESCE(NULLIF(a.PropertyAddress, ''),
            b.PropertyAddress) AS targetAdress
FROM
    housing_stage2 a
        JOIN
    housing_stage2 b ON a.ParcelID = b.ParcelID
        AND a.UniqueID <> b.UniqueID
WHERE
    a.PropertyAddress = ''
        OR a.PropertyAddress IS NULL;

UPDATE housing_stage2 a
        JOIN
    housing_stage2 b ON a.ParcelID = b.ParcelID
        AND a.UniqueID <> b.UniqueID 
SET 
    a.PropertyAddress = COALESCE(NULLIF(a.PropertyAddress, ''),
            b.PropertyAddress)
WHERE
    a.PropertyAddress = ''
        OR a.PropertyAddress IS NULL;

SELECT 
    *
FROM
    housing_stage2
WHERE
    PropertyAddress = ''
        OR PropertyAddress IS NULL;

--------------------------------------------------------------------------

-- Breaking out Address into Individual Columns (Address, City, State)

SELECT 
    PropertyAddress
FROM
    housing_stage2;

SELECT 
    PropertyAddress,
    TRIM(SUBSTRING_INDEX(PropertyAddress, ',', 1)) AS PropertyStreet,
    TRIM(SUBSTRING_INDEX(PropertyAddress, ',', - 1)) AS PropertyCity
FROM
    housing_stage2;

ALTER TABLE housing_stage2
ADD PropertyStreet varchar(255);

UPDATE housing_stage2 
SET 
    PropertyStreet = TRIM(SUBSTRING_INDEX(PropertyAddress, ',', 1));

ALTER TABLE housing_stage2
ADD PropertyCity varchar(255);

UPDATE housing_stage2 
SET 
    PropertyCity = TRIM(SUBSTRING_INDEX(PropertyAddress, ',', - 1));

SELECT 
    *
FROM
    housing_stage2;
    
SELECT 
    OwnerAddress
FROM
    housing_stage2;

SELECT 
    OwnerAddress,
    SUBSTRING(OwnerAddress,
        1,
        LOCATE(',', OwnerAddress) - 1),
    SUBSTRING(OwnerAddress,
        LOCATE(',', OwnerAddress) + 2,
        LOCATE(',',
                OwnerAddress,
                LOCATE(',', OwnerAddress) + 1) - (LOCATE(',', OwnerAddress) + 2)),
    SUBSTRING(OwnerAddress,
        LOCATE(',',
                OwnerAddress,
                LOCATE(',', OwnerAddress) + 1) + 1)
FROM
    housing_stage2;
    
ALTER TABLE housing_stage2
ADD OwnerStreet varchar(255);

UPDATE housing_stage2 
SET 
    OwnerStreet = SUBSTRING(OwnerAddress,
        1,
        LOCATE(',', OwnerAddress) - 1);
    
ALTER TABLE housing_stage2
ADD OwnerCity varchar(255);

UPDATE housing_stage2 
SET 
    OwnerCity = SUBSTRING(OwnerAddress,
        LOCATE(',', OwnerAddress) + 2,
        LOCATE(',',
                OwnerAddress,
                LOCATE(',', OwnerAddress) + 1) - (LOCATE(',', OwnerAddress) + 2));

ALTER TABLE housing_stage2
ADD OwnerState varchar(255);

UPDATE housing_stage2 
SET 
    OwnerState = SUBSTRING(OwnerAddress,
        LOCATE(',',
                OwnerAddress,
                LOCATE(',', OwnerAddress) + 1) + 1);
                
SELECT 
    *
FROM
    housing_stage2;

--------------------------------------------------------------------------

-- Change Y and N to Yes and No in "Sold as Vacant" Field

SELECT DISTINCT(SoldAsVacant), Count(SoldAsVacant)
FROM housing_stage2
Group by SoldAsVacant
ORDER BY 2
;

SELECT 
    SoldAsVacant,
    CASE
        WHEN SoldAsVacant = 'Y' THEN 'Yes'
        WHEN SoldAsVacant = 'N' THEN 'No'
        ELSE SoldAsVacant
    END AS TEST
FROM
    housing_stage2;

UPDATE housing_stage2 
SET 
    SoldAsVacant = CASE
        WHEN SoldAsVacant = 'Y' THEN 'Yes'
        WHEN SoldAsVacant = 'N' THEN 'No'
        ELSE SoldAsVacant
    END
;

--------------------------------------------------------------------------

-- Remove Duplicates
WITH duplicate_CTE as
(
SELECT *, 
	row_number() OVER (
    partition by ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference ORDER BY UniqueID) row_num
FROM housing_stage2
ORDER BY ParcelID
)
SELECT * 
FROM duplicate_CTE
WHERE row_num>1;

SELECT 
    *
FROM
    housing_stage2
WHERE
    ParcelID = '081 07 0 265.00';

CREATE TABLE `housing_stage3` (
    `UniqueID` VARCHAR(50) DEFAULT NULL,
    `ParcelID` VARCHAR(50) DEFAULT NULL,
    `LandUse` VARCHAR(50) DEFAULT NULL,
    `PropertyAddress` VARCHAR(255) DEFAULT NULL,
    `SaleDate` VARCHAR(50) DEFAULT NULL,
    `SalePrice` VARCHAR(50) DEFAULT NULL,
    `LegalReference` VARCHAR(50) DEFAULT NULL,
    `SoldAsVacant` VARCHAR(10) DEFAULT NULL,
    `OwnerName` VARCHAR(255) DEFAULT NULL,
    `OwnerAddress` VARCHAR(255) DEFAULT NULL,
    `Acreage` VARCHAR(50) DEFAULT NULL,
    `TaxDistrict` VARCHAR(100) DEFAULT NULL,
    `LandValue` VARCHAR(50) DEFAULT NULL,
    `BuildingValue` VARCHAR(50) DEFAULT NULL,
    `TotalValue` VARCHAR(50) DEFAULT NULL,
    `YearBuilt` VARCHAR(50) DEFAULT NULL,
    `Bedrooms` VARCHAR(50) DEFAULT NULL,
    `FullBath` VARCHAR(50) DEFAULT NULL,
    `HalfBath` VARCHAR(50) DEFAULT NULL,
    `SaleDateConverted` DATE DEFAULT NULL,
    `PropertyStreet` VARCHAR(255) DEFAULT NULL,
    `PropertyCity` VARCHAR(255) DEFAULT NULL,
    `OwnerStreet` VARCHAR(255) DEFAULT NULL,
    `OwnerCity` VARCHAR(255) DEFAULT NULL,
    `OwnerState` VARCHAR(255) DEFAULT NULL,
    `row_number` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

SELECT 
    *
FROM
    housing_stage3;

INSERT INTO housing_stage3
SELECT *, 
	row_number() OVER (
    partition by 
		ParcelID, 
		PropertyAddress, 
		SalePrice, 
        SaleDate, 
        LegalReference 
        ORDER BY UniqueID) row_num
FROM housing_stage2;

SELECT 
    *
FROM
    housing_stage3
WHERE
    `row_number` > 1;

DELETE FROM housing_stage3 
WHERE
    `row_number` > 1;

SELECT 
    *
FROM
    housing_stage3;

--------------------------------------------------------------------------

-- Delete Unused Columns

SELECT * 
FROM housing_stage3;

DESCRIBE housing_stage3;

ALTER TABLE housing_stage3
DROP COLUMN PropertyAddress,
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict,
DROP COLUMN SaleDate,
DROP COLUMN `row_number`
;

SELECT 
    *
FROM
    housing_stage3;


--------------------------------------------------------------------------


-- 3. Null Value or blank values

SELECT * 
FROM housing_stage3;

SELECT 
    COUNT(*) AS total_rows,
    SUM(HalfBath IS NULL) AS null_count,
    SUM(HalfBath = '') AS empty_string_count,
    SUM(HalfBath = ' 	') AS space_count
FROM
    housing_stage3;

UPDATE housing_stage3 
SET 
    Acreage = NULLIF(TRIM(Acreage), ''),
    LandValue = NULLIF(TRIM(LandValue), ''),
    BuildingValue = NULLIF(TRIM(BuildingValue), ''),
    TotalValue = NULLIF(TRIM(TotalValue), ''),
    YearBuilt = NULLIF(TRIM(YearBuilt), ''),
    Bedrooms = NULLIF(TRIM(Bedrooms), ''),
    FullBath = NULLIF(TRIM(FullBath), ''),
    HalfBath = NULLIF(TRIM(HalfBath), ''),
    OwnerName = NULLIF(OwnerName, ''),
    OwnerStreet = NULLIF(OwnerStreet, ''),
    OwnerCity = NULLIF(OwnerCity, ''),
    OwnerState = NULLIF(OwnerState, '');


--------------------------------------------------------------------------
 

-- Change Type Data & Clean

SELECT * 
FROM housing_stage3; 

DESCRIBE housing_stage3;

SELECT 
    *
FROM
    housing_stage3
WHERE
    SalePrice NOT REGEXP '^[0-9]+(.[0-9]+)?$';

SELECT 
    SalePrice,
    REPLACE(REPLACE(SalePrice, '$', ''),
        ',',
        '') AS CleanSalePrice
FROM
    housing_stage3
;

UPDATE housing_stage3 
SET 
    SalePrice = NULL
WHERE
    TRIM(SalePrice) = '';

UPDATE housing_stage3 
SET 
    SalePrice = TRIM(REPLACE(REPLACE(SalePrice, '$', ''),
            ',',
            ''))
WHERE
    SalePrice IS NOT NULL;

SELECT 
    SalePrice,
    LENGTH(SalePrice) AS len,
    LENGTH(TRIM(SalePrice)) AS trimmed_len
FROM
    housing_stage3
WHERE
    SalePrice IS NOT NULL
        AND SalePrice <> TRIM(SalePrice)
LIMIT 10;

ALTER TABLE housing_stage3
	MODIFY UniqueID BIGINT,
	MODIFY SalePrice BIGINT,
	MODIFY Acreage INT,
	MODIFY LandValue INT,
	MODIFY BuildingValue BIGINT,
	MODIFY TotalValue BIGINT,
	MODIFY YearBuilt INT,
	MODIFY Bedrooms INT,
	MODIFY FullBath INT,
	MODIFY HalfBath INT;

--------------------------------------------------------------------------


