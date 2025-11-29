USE ecommerce_sch;

-- order velocity
SELECT 
    o.OrderDate,
    COUNT(o.OrderFactKey) AS TotalOrders
FROM factorders o
GROUP BY 
    o.OrderDate
ORDER BY 
    o.OrderDate;

-- order quantity and frequency
SELECT 
    f.OrderDate,
    p.ProductName,
    COUNT(f.OrderFactKey) AS OrderFrequency,         
    SUM(s.order_item_quantity) AS TotalQuantity       
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id   
GROUP BY 
    f.OrderDate, 
    p.ProductName
ORDER BY 
    f.OrderDate, 
    p.ProductName;
    
    -- demand variability
  SELECT 
    p.ProductName,
    STDDEV(s.order_item_quantity) AS DemandVariability,  
    AVG(s.order_item_quantity) AS AvgDailyDemand          
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id      
GROUP BY 
    p.ProductName
ORDER BY 
    AvgDailyDemand DESC;

-- lead time analysis
SELECT 
    p.ProductName,
    AVG(DATEDIFF(f.ShippingDate, f.OrderDate)) AS ActualLeadTime,  
    AVG(f.DaysForShipmentScheduled) AS ScheduledLeadTime,      
    (AVG(DATEDIFF(f.ShippingDate, f.OrderDate)) - 
     AVG(f.DaysForShipmentScheduled)) AS LeadTimeDifference        
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
GROUP BY 
    p.ProductName
ORDER BY 
    LeadTimeDifference DESC;

-- reorder point
SELECT 
    p.ProductName,
    (AVG(s.order_item_quantity) * AVG(f.DaysForShipmentScheduled)) 
      + STDDEV(s.order_item_quantity) AS ReorderPoint
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id        
GROUP BY 
    p.ProductName
ORDER BY 
    ReorderPoint DESC;

-- safety stock
SELECT 
    p.ProductName,
    1.65 * STDDEV(s.order_item_quantity) AS SafetyStock
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id
GROUP BY 
    p.ProductName
ORDER BY 
    SafetyStock DESC;

WITH SalesData AS (
    SELECT 
        p.ProductName,
        SUM(s.order_item_quantity * p.ProductPrice) AS TotalSalesValue
    FROM factorders f
    JOIN dimproduct p 
        ON f.ProductKey = p.ProductKey
    JOIN staging_orders s 
        ON f.OrderID = s.Order_id   
    GROUP BY 
        p.ProductName
),
RankedData AS (
    SELECT 
        ProductName,
        TotalSalesValue,
        SUM(TotalSalesValue) OVER () AS GrandTotal,
        SUM(TotalSalesValue) OVER (ORDER BY TotalSalesValue DESC) AS RunningTotal
    FROM SalesData
)

-- ABC analysis
SELECT 
    ProductName,
    TotalSalesValue,
    ROUND((RunningTotal / GrandTotal) * 100, 2) AS CumulativePercent,
    CASE 
        WHEN (RunningTotal / GrandTotal) <= 0.80 THEN 'A'
        WHEN (RunningTotal / GrandTotal) <= 0.95 THEN 'B'     
        ELSE 'C'                                              
    END AS Category
FROM RankedData
ORDER BY 
    TotalSalesValue DESC;

-- stock_out risk
SELECT 
    p.ProductName,
    STDDEV(s.order_item_quantity) AS DemandVariability,
    AVG(s.order_item_quantity) AS AvgDemand,
    (1.65 * STDDEV(s.order_item_quantity)) / NULLIF(AVG(s.order_item_quantity), 0) AS StockoutRiskIndex
FROM factorders f
JOIN dimproduct p 
    ON f.ProductKey = p.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id  
GROUP BY 
    p.ProductName
ORDER BY 
    StockoutRiskIndex DESC;

-- fast_moving vs slow_moving
SELECT 
    p.ProductName,
    COUNT(DISTINCT s.Order_id) AS OrdersCount,
    CASE 
        WHEN COUNT(DISTINCT s.Order_id) > 100 THEN 'Fast-moving'
        ELSE 'Slow-moving'
    END AS ProductSpeed
FROM dimproduct p
JOIN factorders f 
    ON p.ProductKey = f.ProductKey
JOIN staging_orders s 
    ON f.OrderID = s.Order_id
GROUP BY 
    p.ProductName
ORDER BY 
    OrdersCount DESC;
    
-- Product Criticality
WITH Base AS (
    SELECT 
        p.ProductName,
        AVG(s.order_item_quantity) AS AvgDailyDemand,
        AVG(s.days_for_shipment_scheduled) AS AvgLeadTime,
        STDDEV(s.order_item_quantity) AS DemandVariability,
        AVG(p.productprice) AS ProductPrice
    FROM ecommerce_sch.staging_orders s
    JOIN ecommerce_sch.factorders f 
        ON s.order_id = f.OrderId
    JOIN ecommerce_sch.dimproduct p 
        ON s.Product_Name = p.ProductName
    GROUP BY p.ProductName
)

SELECT
    ProductName,
    AvgDailyDemand,
    AvgLeadTime,
    DemandVariability,
    ProductPrice,
    (DemandVariability * SQRT(AvgLeadTime)) AS SafetyStock,
    (AvgDailyDemand * AvgLeadTime) + (DemandVariability * SQRT(AvgLeadTime)) AS ReorderPoint,
    CASE 
        WHEN ProductPrice >= (SELECT AVG(ProductPrice) FROM Base) THEN 'Critical'
        ELSE 'Non-Critical'
    END AS ProductCriticality
FROM Base;
