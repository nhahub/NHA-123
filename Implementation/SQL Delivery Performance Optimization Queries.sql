-- SQL Logistics & Delivery Performance Optimization
-- --------------------------------------------
-- Late delivery Rate ( Valid )
-- 54% 
 CREATE OR REPLACE VIEW Late_delivery_Rate AS
SELECT 
    COUNT(*) AS total_orders,
    SUM(CASE WHEN DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND(
        100.0 * SUM(CASE WHEN DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) AS late_delivery_rate_percent
FROM factorders;

-- Aggregate delivery Status ( Valid ) KPI
CREATE OR REPLACE VIEW Aggregate_shipping_mode AS
SELECT 
    DeliveryStatus,
    COUNT(*) AS total,
    ROUND(
        100.0 * COUNT(*) / (SELECT COUNT(*) FROM factorders 
                            WHERE DaysForShippingReal - DaysForShipmentScheduled IS NOT NULL),
        2
    ) AS percentage
FROM factorders
WHERE DaysForShippingReal - DaysForShipmentScheduled IS NOT NULL
GROUP BY DeliveryStatus;


--  Shipping mode separated ( Valid )
CREATE OR REPLACE VIEW Shipping_mode_separated AS
SELECT
    s.ShippingMode,
    f.DeliveryStatus,
    COUNT(*) AS order_count
FROM factorders f
JOIN dimshippingmode s ON f.ShippingModeKey = s.ShippingModeKey
GROUP BY s.ShippingMode, f.DeliveryStatus
ORDER BY s.ShippingMode, f.DeliveryStatus;

-- Late delivery rate for each shipping mode ( Valid )
-- First class and second class are the highest late delivery rate so should be stop temporary and replace it to standard Class.
CREATE OR REPLACE VIEW Late_delivery_rate_for_each_shipping_mode AS
SELECT
    dsm.ShippingMode,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN fo.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND(
        100.0 * SUM(CASE WHEN fo.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) AS late_delivery_percent
FROM factorders fo
JOIN dimshippingmode dsm ON fo.ShippingModeKey = dsm.ShippingModeKey
GROUP BY dsm.ShippingMode
ORDER BY late_delivery_percent DESC;


-- Geographic market ( Valid )
-- Focus On:
-- Latin America Total_orders 25819 / Rate 54.21%
-- Europe Total_orders 21464 / Rate 54.91%
CREATE OR REPLACE VIEW Geographic_market AS
SELECT 
    l.Market,
    SUM(CASE WHEN f.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) AS late_deliveries,
    COUNT(*) AS total_orders,
    ROUND(100.0 * SUM(CASE WHEN f.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_delivery_rate_percent
FROM factorders f
JOIN dimlocation l ON f.LocationKey = l.LocationKey
GROUP BY l.Market
ORDER BY late_delivery_rate_percent DESC;



-- On-time delivery rate ( Valid )
-- 18%
-- Advanced Ship 
-- 23%
-- Solution shrink advanced ship and replace it to on time so percentage be 18% + 23% = 41% ( The main goal is make customer satisfied not delight )
CREATE OR REPLACE VIEW On_time_delivery_rate AS
SELECT 
    SUM(CASE WHEN DeliveryStatus = 'Shipping On Time' THEN 1 ELSE 0 END) AS ontime_deliveries,
    COUNT(*) AS total_orders,
    ROUND(
        100.0 * SUM(CASE WHEN DeliveryStatus = 'Shipping On Time' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) AS ontime_delivery_rate_percent
FROM factorders;

-- Average delay late orders ( Valid )
-- Identify the problem Operational or Planning 
CREATE OR REPLACE VIEW Average_delay_late_orders AS
SELECT 
    ROUND(AVG(DaysForShippingReal - DaysForShipmentScheduled), 2) AS avg_delay_late_orders
FROM factorders
WHERE DaysForShippingReal > DaysForShipmentScheduled;


-- Shipping mode KPI ( Valid )

CREATE OR REPLACE VIEW Shipping_mode_KPI AS
SELECT
  s.ShippingModeKey,
  s.ShippingMode,
  COUNT(*) AS OrdersCount,
  SUM(f.Sales) AS TotalSales,
  AVG(f.DaysForShippingReal) AS AvgActualShippingDays,
  AVG(f.DaysForShipmentScheduled) AS AvgScheduledShippingDays,
  SUM(CASE WHEN f.DaysForShippingReal > f.DaysForShipmentScheduled THEN 1 ELSE 0 END) 
    / COUNT(*) AS LateRate,
  AVG(CASE WHEN f.DaysForShippingReal > f.DaysForShipmentScheduled 
           THEN (f.DaysForShippingReal - f.DaysForShipmentScheduled) END) AS AvgDelayWhenLate
FROM factorders f
JOIN dimshippingmode s 
    ON f.ShippingModeKey = s.ShippingModeKey
GROUP BY s.ShippingModeKey, s.ShippingMode
ORDER BY OrdersCount DESC;



-- KPI Summary ( Valid )

CREATE OR REPLACE VIEW KPI_SUMMARY AS
SELECT
    SUM(CASE WHEN DeliveryStatus = 'Late Delivery' THEN 1 ELSE 0 END) AS LateOrders,
    SUM(CASE WHEN DeliveryStatus = 'Late Delivery' THEN Sales ELSE 0 END) AS LateOrdersSales,
    AVG(CASE WHEN DeliveryStatus = 'Late Delivery' THEN DaysForShippingReal END) AS AvgActualShippingDays_LateOnly,
    AVG(CASE WHEN DeliveryStatus = 'Late Delivery' THEN DaysForShipmentScheduled END) AS AvgScheduledDays_LateOnly,
    AVG(CASE WHEN DeliveryStatus = 'Late Delivery' THEN (DaysForShippingReal - DaysForShipmentScheduled) END) AS AvgDelayWhenLate
FROM factorders;


Select *
From factorders Limit 5;


-- Geographic Makret ALL ( Valid ) 
CREATE OR REPLACE VIEW Geo_All AS 
SELECT 
    l.Country,
    l.State,
    SUM(CASE WHEN f.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) AS late_deliveries,
    COUNT(*) AS total_orders,
    ROUND(
        100.0 * SUM(CASE WHEN f.DeliveryStatus = 'Late delivery' THEN 1 ELSE 0 END) 
        / COUNT(*), 
        2
    ) AS late_delivery_rate_percent
FROM factorders f
JOIN dimlocation l 
    ON f.LocationKey = l.LocationKey
GROUP BY l.Country, l.State
ORDER BY late_delivery_rate_percent DESC;


-- Advance shipping 
CREATE OR REPLACE VIEW Advance_Shipping AS 
SELECT 
    'Advance Shipping' AS DeliveryStatus,
    COUNT(*) AS total,
    ROUND(
        100.0 * COUNT(*) / (SELECT COUNT(*) 
                            FROM factorders
                            WHERE DaysForShippingReal - DaysForShipmentScheduled IS NOT NULL),
        2
    ) AS advance_rate_percent
FROM factorders
WHERE DeliveryStatus = 'Advance Shipping'
  AND DaysForShippingReal - DaysForShipmentScheduled IS NOT NULL;



