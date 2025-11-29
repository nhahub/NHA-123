-- =====================================================
-- STEP 1: CREATE DATABASE
-- =====================================================
DROP DATABASE IF EXISTS ecommerce_Sch;
CREATE DATABASE ecommerce_Sch;
USE ecommerce_Sch;

-- =====================================================
-- STEP 2: DIMENSION TABLES
-- =====================================================

-- 1. DimCustomer
CREATE TABLE DimCustomer (
    CustomerKey INT AUTO_INCREMENT PRIMARY KEY,
    CustomerId VARCHAR(50) NOT NULL UNIQUE,
    FullName VARCHAR(255),
    CustomerSegment VARCHAR(50),
    CustomerStreet VARCHAR(255),
    CustomerCity VARCHAR(100),
    CustomerState VARCHAR(100),
    CustomerZipcode VARCHAR(20),
    CustomerCountry VARCHAR(100),
    INDEX idx_customer_id (CustomerId),
    INDEX idx_customer_segment (CustomerSegment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. DimCategory
CREATE TABLE DimCategory (
    CategoryKey INT AUTO_INCREMENT PRIMARY KEY,
    CategoryId VARCHAR(50) UNIQUE,
    CategoryName VARCHAR(255) NOT NULL,
    INDEX idx_category_name (CategoryName)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. DimDepartment
CREATE TABLE DimDepartment (
    DepartmentKey INT AUTO_INCREMENT PRIMARY KEY,
    DepartmentId VARCHAR(50) UNIQUE,
    DepartmentName VARCHAR(255) NOT NULL,
    INDEX idx_department_name (DepartmentName)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. DimProduct
CREATE TABLE DimProduct (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    ProductCardId VARCHAR(50) NOT NULL UNIQUE,
    ProductName VARCHAR(500),
    ProductPrice DECIMAL(10, 2),
    ProductImage VARCHAR(500),
    CategoryKey INT,
    DepartmentKey INT,
    FOREIGN KEY (CategoryKey) REFERENCES DimCategory(CategoryKey),
    FOREIGN KEY (DepartmentKey) REFERENCES DimDepartment(DepartmentKey),
    INDEX idx_product_name (ProductName(255)),
    INDEX idx_category_key (CategoryKey),
    INDEX idx_department_key (DepartmentKey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. DimLocation
CREATE TABLE DimLocation (
    LocationKey INT AUTO_INCREMENT PRIMARY KEY,
    Market VARCHAR(100),
    OrderRegion VARCHAR(100),
    Country VARCHAR(100),
    State VARCHAR(100),
    City VARCHAR(100),
    Latitude DECIMAL(10, 6),
    Longitude DECIMAL(10, 6),
    INDEX idx_market (Market),
    INDEX idx_region (OrderRegion),
    INDEX idx_country_state (Country, State)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. DimShippingMode
CREATE TABLE DimShippingMode (
    ShippingModeKey INT AUTO_INCREMENT PRIMARY KEY,
    ShippingMode VARCHAR(100) NOT NULL UNIQUE,
    INDEX idx_shipping_mode (ShippingMode)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- =====================================================
-- STEP 3: FACT TABLES
-- =====================================================

-- 1. FactOrders
CREATE TABLE FactOrders (
    OrderFactKey BIGINT AUTO_INCREMENT PRIMARY KEY,
    OrderId VARCHAR(50) NOT NULL,
    
    -- Foreign Keys to Dimensions
    CustomerKey INT,
    ProductKey INT,
    LocationKey INT,
    ShippingModeKey INT,
    
    -- Date and Time directly in Fact
    OrderDate DATE,
    OrderTime TIME,
    ShippingDate DATE,
    ShippingTime TIME,
    
    -- Order Metrics
    OrderStatus VARCHAR(50),
    OrderItemQuantity INT,
    Sales DECIMAL(12, 2),
    OrderItemDiscount DECIMAL(12, 2),
    OrderItemDiscountRate DECIMAL(5, 4),
    OrderItemProductPrice DECIMAL(10, 2),
    OrderItemProfitRatio DECIMAL(5, 4),
    OrderProfitPerOrder DECIMAL(12, 2),
    SalesPerCustomer DECIMAL(12, 2),
    
    -- Shipping Metrics
    DaysForShippingReal INT,
    DaysForShipmentScheduled INT,
    DeliveryStatus VARCHAR(50),
    LateDeliveryRisk INT,
    
    -- Payment Information
    PaymentType VARCHAR(20),
    
    -- Foreign Key Constraints
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (LocationKey) REFERENCES DimLocation(LocationKey),
    FOREIGN KEY (ShippingModeKey) REFERENCES DimShippingMode(ShippingModeKey),
    
    -- Indexes for Performance
    INDEX idx_order_id (OrderId),
    INDEX idx_customer_key (CustomerKey),
    INDEX idx_product_key (ProductKey),
    INDEX idx_order_status (OrderStatus),
    INDEX idx_delivery_status (DeliveryStatus)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. FactWebLogs
CREATE TABLE FactWebLogs (
    LogKey BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    ProductKey INT,
    CategoryKey INT,
    DepartmentKey INT,
    
    -- Date and Time 
    LogDate DATE,
    LogTime TIME,
    
    -- Log Details
    IPAddress VARCHAR(50),
    URL VARCHAR(1000),
    
    -- Foreign Key Constraints
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (CategoryKey) REFERENCES DimCategory(CategoryKey),
    FOREIGN KEY (DepartmentKey) REFERENCES DimDepartment(DepartmentKey),
    
    -- Indexes
    INDEX idx_product_key (ProductKey),
    INDEX idx_category_key (CategoryKey),
    INDEX idx_ip_address (IPAddress)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



-- =====================================================
-- STAGING TABLES FOR DATA LOADING
-- =====================================================

-- Staging table for orders_copy.csv
CREATE TABLE staging_orders (
    type VARCHAR(255),
    days_for_shipping_real VARCHAR(255),
    days_for_shipment_scheduled VARCHAR(255),
    sales_per_customer VARCHAR(255),
    delivery_status VARCHAR(255),
    late_delivery_risk VARCHAR(255),
    category_id VARCHAR(255),
    category_name VARCHAR(255),
    customer_city VARCHAR(255),
    customer_country VARCHAR(255),
    full_name VARCHAR(255),
    customer_id VARCHAR(255),
    customer_segment VARCHAR(50),
    customer_state VARCHAR(255),
    customer_street VARCHAR(255),
    customer_zipcode VARCHAR(255),
    department_id VARCHAR(255),
    department_name VARCHAR(255),
    latitude VARCHAR(255),
    longitude VARCHAR(255),
    market VARCHAR(255),
    order_city VARCHAR(255),
    order_country VARCHAR(255),
    order_customer_id VARCHAR(255),
    order_date VARCHAR(255),
    order_year VARCHAR(255),
    order_time VARCHAR(255),
    order_id VARCHAR(255),
    order_item_cardprod_id VARCHAR(255),
    order_item_discount VARCHAR(255),
    order_item_discount_rate VARCHAR(255),
    order_item_id VARCHAR(255),
    order_item_product_price VARCHAR(255),
    order_item_profit_ratio VARCHAR(255),
    order_item_quantity VARCHAR(255),
    sales VARCHAR(255),
    order_item_total_discount VARCHAR(255),
    order_profit_per_order VARCHAR(255),
    order_region VARCHAR(255),
    order_state VARCHAR(255),
    order_status VARCHAR(255),
    order_zipcode VARCHAR(255),
    product_card_id VARCHAR(255),
    product_category_id VARCHAR(255),
    product_image VARCHAR(255),
    product_name VARCHAR(255),
    product_price VARCHAR(255),
    shipping_date VARCHAR(255),
    shipping_time VARCHAR(255),
    shipping_mode VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Staging table for logss_copy.csv
CREATE TABLE staging_logs (
    Product VARCHAR(500),
    Category VARCHAR(500),
    LogDate VARCHAR(500),
    LogTime VARCHAR(500),
    LogYear VARCHAR(500),
    LogMonth VARCHAR(500),
    LogQuarter VARCHAR(500),
    LogHour VARCHAR(500),
    Department VARCHAR(500),
    IPAddress VARCHAR(500),
    URL VARCHAR(1000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- LOAD CSV DATA INTO STAGING TABLES
-- =====================================================

TRUNCATE TABLE staging_logs;
TRUNCATE TABLE staging_orders;

SET GLOBAL local_infile = 1;

-- NOTE: Change file path before running
-- Load logss_copy.csv into staging_logs
LOAD DATA LOCAL INFILE 'C:/DEPI project/logs_access_2017_.csv'
INTO TABLE staging_logs
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Product, Category, LogDate, LogTime, LogYear, LogMonth, LogQuarter, LogHour, Department, IPAddress, URL);

-- NOTE: Change file path before running
LOAD DATA LOCAL INFILE "C:/Users/hager/Downloads/orders_2017.csv"
INTO TABLE staging_orders
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(type, days_for_shipping_real, days_for_shipment_scheduled, sales_per_customer,
 delivery_status, late_delivery_risk, category_id, category_name, customer_city,
 customer_country, full_name, customer_id, customer_segment, customer_state,
 customer_street, customer_zipcode, department_id, department_name, latitude,
 longitude, market, order_city, order_country, order_customer_id, order_date,
 order_year, order_time, order_id, order_item_cardprod_id, order_item_discount,
 order_item_discount_rate, order_item_id, order_item_product_price,
 order_item_profit_ratio, order_item_quantity, sales, order_item_total_discount,
 order_profit_per_order, order_region, order_state, order_status, order_zipcode,
 product_card_id, product_category_id, product_image, product_name, product_price,
 shipping_date, shipping_time, shipping_mode);

-- 2.1 Populate DimCustomer
INSERT INTO DimCustomer (CustomerId, FullName, CustomerSegment, CustomerStreet, 
                         CustomerCity, CustomerState, CustomerZipcode, CustomerCountry)
SELECT DISTINCT
    TRIM(customer_id) AS CustomerId,
    TRIM(full_name) AS FullName,
    TRIM(customer_segment) AS CustomerSegment,
    TRIM(customer_street) AS CustomerStreet,
    TRIM(customer_city) AS CustomerCity,
    TRIM(customer_state) AS CustomerState,
    TRIM(customer_zipcode) AS CustomerZipcode,
    TRIM(customer_country) AS CustomerCountry
FROM staging_orders
WHERE customer_id IS NOT NULL 
  AND TRIM(customer_id) != ''
ON DUPLICATE KEY UPDATE
    FullName = VALUES(FullName),
    CustomerSegment = VALUES(CustomerSegment),
    CustomerStreet = VALUES(CustomerStreet),
    CustomerCity = VALUES(CustomerCity),
    CustomerState = VALUES(CustomerState),
    CustomerZipcode = VALUES(CustomerZipcode),
    CustomerCountry = VALUES(CustomerCountry);

-- 2.2 Populate DimCategory from orders
INSERT INTO DimCategory (CategoryId, CategoryName)
SELECT DISTINCT
     MIN(TRIM(category_id)) AS CategoryId,
    TRIM(category_name) AS CategoryName
FROM staging_orders
WHERE category_id IS NOT NULL 
  AND TRIM(category_id) != ''
  AND category_name IS NOT NULL
  AND TRIM(category_name) != ''
  GROUP BY TRIM(category_name)
ON DUPLICATE KEY UPDATE
    CategoryName = VALUES(CategoryName);
    
    -- 2.3 Populate DimDepartment from orders
INSERT INTO DimDepartment (DepartmentId, DepartmentName)
SELECT DISTINCT
    TRIM(department_id) AS DepartmentId,
    TRIM(department_name) AS DepartmentName
FROM staging_orders
WHERE department_id IS NOT NULL 
  AND TRIM(department_id) != ''
  AND department_name IS NOT NULL
  AND TRIM(department_name) != ''
ON DUPLICATE KEY UPDATE
    DepartmentName = VALUES(DepartmentName);

-- Add *new* departments from logs that don't already exist
INSERT INTO DimDepartment (DepartmentId, DepartmentName)
SELECT DISTINCT
    NULL AS DepartmentId,
    TRIM(Department) AS DepartmentName
FROM staging_logs l
WHERE Department IS NOT NULL 
  AND TRIM(Department) != ''
  AND NOT EXISTS (
        SELECT 1 
        FROM DimDepartment d 
        WHERE d.DepartmentName = TRIM(l.Department)
    );

-- 2.4 Populate DimProduct from orders
INSERT INTO DimProduct (ProductCardId, ProductName, ProductPrice, ProductImage, 
                        CategoryKey, DepartmentKey)
SELECT DISTINCT
    TRIM(s.product_card_id) AS ProductCardId,
    TRIM(s.product_name) AS ProductName,
    CASE 
        WHEN s.product_price REGEXP '^[0-9]+\.?[0-9]*$' 
        THEN CAST(s.product_price AS DECIMAL(10,2))
        ELSE 0 
    END AS ProductPrice,
    CASE 
        WHEN TRIM(s.product_image) != '' THEN TRIM(s.product_image)
        ELSE 'Unknown'
    END AS ProductImage,
    c.CategoryKey,
    d.DepartmentKey
FROM staging_orders s
LEFT JOIN DimCategory c ON TRIM(s.category_name) = c.CategoryName
LEFT JOIN DimDepartment d ON TRIM(s.department_name) = d.DepartmentName
WHERE s.product_card_id IS NOT NULL 
  AND TRIM(s.product_card_id) != ''
ON DUPLICATE KEY UPDATE
    ProductName = VALUES(ProductName),
    ProductPrice = VALUES(ProductPrice),
    ProductImage = VALUES(ProductImage),
    CategoryKey = VALUES(CategoryKey),
    DepartmentKey = VALUES(DepartmentKey);

-- Add products from logs that don't exist in orders or DimProduct yet
INSERT INTO DimProduct (ProductCardId, ProductName, ProductPrice, ProductImage, CategoryKey, DepartmentKey)
SELECT DISTINCT
    MD5(TRIM(l.Product)) AS ProductCardId,   
    TRIM(l.Product) AS ProductName,
    0 AS ProductPrice,
    'Unknown' AS ProductImage,
    c.CategoryKey,
    d.DepartmentKey
FROM staging_logs l
LEFT JOIN DimCategory c ON TRIM(l.Category) = c.CategoryName
LEFT JOIN DimDepartment d ON TRIM(l.Department) = d.DepartmentName
WHERE l.Product IS NOT NULL
  AND TRIM(l.Product) != ''
  AND TRIM(l.Product) NOT IN (SELECT ProductName FROM DimProduct WHERE ProductName IS NOT NULL);

-- 2.5 Populate DimLocation
INSERT INTO DimLocation (Market, OrderRegion, Country, State, City, Latitude, Longitude)
SELECT DISTINCT
    TRIM(market) AS Market,
    TRIM(order_region) AS OrderRegion,
    TRIM(order_country) AS Country,
    TRIM(order_state) AS State,
    TRIM(order_city) AS City,
    CASE 
        WHEN latitude REGEXP '^-?[0-9]+\.?[0-9]*$' 
        THEN CAST(latitude AS DECIMAL(10,6))
        ELSE NULL 
    END AS Latitude,
    CASE 
        WHEN longitude REGEXP '^-?[0-9]+\.?[0-9]*$' 
        THEN CAST(longitude AS DECIMAL(10,6))
        ELSE NULL 
    END AS Longitude
FROM staging_orders
WHERE market IS NOT NULL OR order_region IS NOT NULL;

-- 2.6 Populate DimShippingMode
INSERT INTO DimShippingMode (ShippingMode)
SELECT DISTINCT
    TRIM(shipping_mode) AS ShippingMode
FROM staging_orders
WHERE shipping_mode IS NOT NULL 
  AND TRIM(shipping_mode) != ''
ON DUPLICATE KEY UPDATE
    ShippingMode = VALUES(ShippingMode);

-- =====================================================
-- STEP 3: POPULATE FACT TABLES
-- =====================================================

-- 3.2 Populate FactWebLogs
INSERT INTO FactWebLogs (
    ProductKey, CategoryKey, DepartmentKey, LogDate, LogTime, IPAddress, URL
)
SELECT 
    p.ProductKey,
    c.CategoryKey,
    d.DepartmentKey,
    
    -- Date and time conversions
   STR_TO_DATE(l.LogDate, '%d/%m/%Y') AS LogDate, -- لو التاريخ يوم/شهر/سنة
CASE 
    WHEN l.LogTime REGEXP '^[0-9]{1,2}:[0-9]{2}:[0-9]{2} [صم]$' THEN
        STR_TO_DATE(REPLACE(REPLACE(TRIM(l.LogTime), 'ص', 'AM'), 'م', 'PM'), '%h:%i:%s %p')
    WHEN l.LogTime REGEXP '^[0-9]{1,2}:[0-9]{2}$' THEN
        CAST(CONCAT(l.LogTime, ':00') AS TIME)
    ELSE NULL
END AS LogTime,

    TRIM(l.IPAddress) AS IPAddress,
    TRIM(l.URL) AS URL
    
FROM staging_logs l
LEFT JOIN DimProduct p ON TRIM(l.Product) = p.ProductName
LEFT JOIN DimCategory c ON TRIM(l.Category) = c.CategoryName
LEFT JOIN DimDepartment d ON TRIM(l.Department) = d.DepartmentName
WHERE l.LogDate IS NOT NULL;
-- ------------------------------------------------------------------------------
-- Dimlocation reset
-- Backup DimLocation first
CREATE TABLE IF NOT EXISTS DimLocation_backup AS SELECT * FROM DimLocation;

-- Create a clean DimLocation table
DROP TABLE IF EXISTS DimLocation_clean;
CREATE TABLE DimLocation_clean (
    LocationKey INT AUTO_INCREMENT PRIMARY KEY,
    Market VARCHAR(100),
    OrderRegion VARCHAR(100),
    Country VARCHAR(100),
    State VARCHAR(100),
    City VARCHAR(100),
    Latitude DECIMAL(10, 6),
    Longitude DECIMAL(10, 6),
    UNIQUE KEY unique_location (Market, OrderRegion, Country, State, City),
    INDEX idx_market (Market),
    INDEX idx_region (OrderRegion),
    INDEX idx_country_state (Country, State)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert unique locations with proper handling
INSERT INTO DimLocation_clean (Market, OrderRegion, Country, State, City, Latitude, Longitude)
SELECT 
    TRIM(COALESCE(Market, '')) AS Market,
    TRIM(COALESCE(OrderRegion, '')) AS OrderRegion,
    TRIM(COALESCE(Country, '')) AS Country,
    TRIM(COALESCE(State, '')) AS State,
    TRIM(COALESCE(City, '')) AS City,
    AVG(Latitude) AS Latitude,  -- Average if multiple coordinates
    AVG(Longitude) AS Longitude
FROM DimLocation
GROUP BY 
    TRIM(COALESCE(Market, '')),
    TRIM(COALESCE(OrderRegion, '')),
    TRIM(COALESCE(Country, '')),
    TRIM(COALESCE(State, '')),
    TRIM(COALESCE(City, ''))
ON DUPLICATE KEY UPDATE
    Latitude = VALUES(Latitude),
    Longitude = VALUES(Longitude);

-- Replace old table with clean one
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE DimLocation;
RENAME TABLE DimLocation_clean TO DimLocation;
SET FOREIGN_KEY_CHECKS = 1;
SELECT 'DimLocation cleaned' AS Status, COUNT(*) AS Records FROM DimLocation;


-- ============================================
-- PHASE 1: Pre-clean the staging table
-- ============================================

ALTER TABLE staging_orders
ADD COLUMN clean_customer_id VARCHAR(50),
ADD COLUMN clean_product_id VARCHAR(50),
ADD COLUMN clean_market VARCHAR(50),
ADD COLUMN clean_region VARCHAR(50),
ADD COLUMN clean_country VARCHAR(50),
ADD COLUMN clean_shipping_mode VARCHAR(50);


CREATE INDEX idx_customer_id ON staging_orders(customer_id);
CREATE INDEX idx_product_card_id ON staging_orders(product_card_id);
CREATE INDEX idx_location ON staging_orders(market, order_region, order_country);
CREATE INDEX idx_shipping_mode ON staging_orders(shipping_mode);

-- ============================================
-- PHASE 2: Convert OrderTime and ShippingTime
-- ============================================

-- add new table 
ALTER TABLE staging_orders
ADD COLUMN converted_order_time TIME,
ADD COLUMN converted_shipping_time TIME;

-- edit time and date
UPDATE staging_orders
SET 
    converted_order_time = CASE 
        WHEN order_time REGEXP '^[0-9]{1,2}:[0-9]{2}:[0-9]{2} [صم]$' THEN
            STR_TO_DATE(REPLACE(REPLACE(TRIM(order_time), 'ص', 'AM'), 'م', 'PM'), '%h:%i:%s %p')
        WHEN order_time REGEXP '^[0-9]{1,2}:[0-9]{2}$' THEN
            CAST(CONCAT(order_time, ':00') AS TIME)
        ELSE NULL
    END,
    converted_shipping_time = CASE 
        WHEN shipping_time REGEXP '^[0-9]{1,2}:[0-9]{2}:[0-9]{2} [صم]$' THEN
            STR_TO_DATE(REPLACE(REPLACE(TRIM(shipping_time), 'ص', 'AM'), 'م', 'PM'), '%h:%i:%s %p')
        WHEN shipping_time REGEXP '^[0-9]{1,2}:[0-9]{2}$' THEN
            CAST(CONCAT(shipping_time, ':00') AS TIME)
        ELSE NULL
    END
WHERE order_id IS NOT NULL AND order_id != '';

SELECT 'Phase 2 Complete: Time conversions done' AS Status;

SET SQL_SAFE_UPDATES = 0;
UPDATE staging_orders
SET late_delivery_risk = CASE 
        WHEN late_delivery_risk = 'TRUE' THEN '1'
        WHEN late_delivery_risk = 'FALSE' THEN '0'
        ELSE '0'
    END;
    SET SQL_SAFE_UPDATES = 1;

-- ============================================
-- PHASE 3:into FactOrders
-- ============================================

INSERT INTO FactOrders (
    OrderId, CustomerKey, ProductKey, LocationKey, ShippingModeKey,
    OrderDate, OrderTime, ShippingDate, ShippingTime,
    OrderStatus, OrderItemQuantity, Sales, OrderItemDiscount,
    OrderItemDiscountRate, OrderItemProductPrice, OrderItemProfitRatio,
    OrderProfitPerOrder, SalesPerCustomer, DaysForShippingReal,
    DaysForShipmentScheduled, DeliveryStatus, LateDeliveryRisk, PaymentType
)
SELECT 
    TRIM(s.order_id),
    c.CustomerKey,
    p.ProductKey,
    l.LocationKey,
    sm.ShippingModeKey,
    
-- Date and time
    STR_TO_DATE(s.order_date, '%d/%m/%Y'),
    s.converted_order_time,
    STR_TO_DATE(s.shipping_date, '%d/%m/%Y'),
    s.converted_shipping_time,
    

    TRIM(s.order_status),
    CAST(s.order_item_quantity AS SIGNED),
    CAST(s.sales AS DECIMAL(12,2)),
    CAST(s.order_item_discount AS DECIMAL(12,2)),
    CAST(s.order_item_discount_rate AS DECIMAL(5,4)),
    CAST(s.order_item_product_price AS DECIMAL(10,2)),
    CAST(s.order_item_profit_ratio AS DECIMAL(5,4)),
    CAST(s.order_profit_per_order AS DECIMAL(12,2)),
    CAST(s.sales_per_customer AS DECIMAL(12,2)),
    CAST(s.days_for_shipping_real AS SIGNED),
    CAST(s.days_for_shipment_scheduled AS SIGNED),
    TRIM(s.delivery_status),
    CAST(s.late_delivery_risk AS SIGNED),
    TRIM(s.type)
FROM staging_orders s
LEFT JOIN DimCustomer c ON TRIM(s.customer_id) = c.CustomerId
LEFT JOIN DimProduct p ON TRIM(s.product_card_id) = p.ProductCardId
LEFT JOIN DimLocation l 
    ON TRIM(COALESCE(s.market, '')) = TRIM(COALESCE(l.Market, ''))
    AND TRIM(COALESCE(s.order_region, '')) = TRIM(COALESCE(l.OrderRegion, ''))
    AND TRIM(COALESCE(s.order_country, '')) = TRIM(COALESCE(l.Country, ''))
    AND TRIM(COALESCE(s.order_state, '')) = TRIM(COALESCE(l.State, ''))
    AND TRIM(COALESCE(s.order_city, '')) = TRIM(COALESCE(l.City, ''))
LEFT JOIN DimShippingMode sm ON TRIM(s.shipping_mode) = sm.ShippingMode
WHERE s.order_id IS NOT NULL AND s.order_id != '';

SELECT 'Phase 3 Complete: FactOrders populated' AS Status, 
       COUNT(*) AS RecordCount 
FROM FactOrders;
-- --------------------------------------------------------------------------------------------
-- edits
DELETE FROM FactWebLogs
WHERE ProductKey= 129;

UPDATE FactWebLogs
SET CategoryKey = 4
WHERE ProductKey = 4
AND CategoryKey IS NULL;

-- Check for orphaned records in FactOrders
SELECT 'Orders with missing Customer:' AS oCheck, COUNT(*) AS Count 
FROM FactOrders WHERE CustomerKey IS NULL;

SELECT 'Orders with missing Product:' AS oCheck, COUNT(*) AS Count 
FROM FactOrders WHERE ProductKey IS NULL;

SELECT 'Orders with missing Location:' AS jCheck, COUNT(*) AS Count 
FROM FactOrders WHERE LocationKey IS NULL;

-- Check for orphaned records in FactWebLogs
SELECT 'Logs with missing Product:' AS kCheck, COUNT(*) AS Count 
FROM FactWebLogs WHERE ProductKey IS NULL;

SELECT 'Logs with missing Category:' AS uCheck, COUNT(*) AS Count 
FROM FactWebLogs WHERE CategoryKey IS NULL;


SELECT TABLE_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'ecommerce_sch'
  AND REFERENCED_TABLE_NAME IS NOT NULL;
  
SELECT COUNT(*) FROM staging_orders;
SELECT COUNT(*) FROM FactOrders;

SHOW TABLES;

SELECT '========== DATA QUALITY REPORT ==========' AS Report;

-- Check for NULL foreign keys in FactOrders
SELECT 
    'FactOrders Data Quality' AS TableName,
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN CustomerKey IS NULL THEN 1 ELSE 0 END) AS MissingCustomer,
    SUM(CASE WHEN ProductKey IS NULL THEN 1 ELSE 0 END) AS MissingProduct,
    SUM(CASE WHEN LocationKey IS NULL THEN 1 ELSE 0 END) AS MissingLocation,
    SUM(CASE WHEN ShippingModeKey IS NULL THEN 1 ELSE 0 END) AS MissingShippingMode
FROM FactOrders;

-- Check for NULL foreign keys in FactWebLogs
SELECT 
    'FactWebLogs Data Quality' AS TableName,
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN ProductKey IS NULL THEN 1 ELSE 0 END) AS MissingProduct,
    SUM(CASE WHEN CategoryKey IS NULL THEN 1 ELSE 0 END) AS MissingCategory,
    SUM(CASE WHEN DepartmentKey IS NULL THEN 1 ELSE 0 END) AS MissingDepartment
FROM FactWebLogs;

-- Summary of all tables
SELECT '========== FINAL RECORD COUNTS ==========' AS Summary;
SELECT 'DimCustomer' AS TableName, COUNT(*) AS Records FROM DimCustomer
UNION ALL SELECT 'DimCategory', COUNT(*) FROM DimCategory
UNION ALL SELECT 'DimDepartment', COUNT(*) FROM DimDepartment
UNION ALL SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL SELECT 'DimLocation', COUNT(*) FROM DimLocation
UNION ALL SELECT 'DimShippingMode', COUNT(*) FROM DimShippingMode
UNION ALL SELECT 'FactOrders', COUNT(*) FROM FactOrders
UNION ALL SELECT 'FactWebLogs', COUNT(*) FROM FactWebLogs;





    
   





