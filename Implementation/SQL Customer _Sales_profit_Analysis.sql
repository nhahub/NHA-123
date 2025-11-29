use ecommerce_sch;

-- ================================================
-- customer Analysis
-- ================================================

 CREATE OR REPLACE VIEW v_customer_base AS
SELECT 
    c.CustomerKey,
    c.CustomerId,
    c.FullName,
    c.CustomerSegment,
    c.CustomerCity,
    c.CustomerState,
    c.CustomerCountry,
    o.OrderId,
    o.OrderDate,
    o.Sales,
    o.OrderItemDiscount,
    o.OrderItemProfitRatio,
    o.OrderItemQuantity,
    o.PaymentType,
    o.DaysForShippingReal,
    o.DaysForShipmentScheduled
FROM factorders o
JOIN dimcustomer c ON o.CustomerKey = c.CustomerKey;

-- =========================================
-- summary table by segmant
-- =========================================
 CREATE OR REPLACE VIEW v_customer_segment_summary AS
SELECT 
    c.CustomerSegment,
    COUNT(DISTINCT o.OrderId) AS Total_Orders,
    COUNT(DISTINCT c.CustomerKey) AS Total_Customers,
    SUM(o.Sales) AS Total_Sales,
    SUM(o.SalesPerCustomer) AS Total_Revenue,
    SUM(o.OrderProfitPerOrder) AS Total_Profit,
    ROUND((SUM(o.OrderProfitPerOrder)) * 100.0 / (SUM(o.SalesPerCustomer)), 2) AS Profit_Margin,
    ROUND(AVG(o.SalesPerCustomer), 2) AS Avg_Order_Value,
    ROUND((AVG(o.SalesPerCustomer)) * 
          (COUNT(DISTINCT o.OrderId) / COUNT(DISTINCT c.CustomerKey)) * 
          ((SUM(o.OrderProfitPerOrder)) / NULLIF(SUM(o.SalesPerCustomer), 0)), 2) AS Estimated_CLV,
    ROUND(SUM(o.SalesPerCustomer) / COUNT(DISTINCT o.OrderId), 2) AS Revenue_Per_Order,
    COUNT(DISTINCT CASE WHEN t.cnt > 1 THEN t.CustomerKey END) AS Repeat_Customers,
    ROUND(COUNT(DISTINCT CASE WHEN t.cnt > 1 THEN t.CustomerKey END) * 100.0 /
          COUNT(DISTINCT t.CustomerKey), 2) AS Repeat_Rate
FROM factorders o
LEFT JOIN dimcustomer c 
       ON o.CustomerKey = c.CustomerKey
LEFT JOIN (
    SELECT CustomerKey, COUNT(DISTINCT OrderId) AS cnt
    FROM factorders
    WHERE OrderStatus = 'Complete'
    GROUP BY CustomerKey
) t ON o.CustomerKey = t.CustomerKey
WHERE o.OrderStatus = 'Complete'
GROUP BY c.CustomerSegment;

-- Global Benchmark for AOV and CLV across all segments
SELECT 
    ROUND(AVG(Avg_Order_Value), 2) AS Global_AOV,
    ROUND(AVG(Estimated_CLV), 2) AS Global_CLV
FROM v_customer_segment_summary;

-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_customer_segment_products AS
SELECT 
    c.CustomerSegment,
    p.ProductName,
    cat.CategoryName,
    COUNT(o.OrderId) AS Orders_Count,
    SUM(o.SalesPerCustomer) AS Total_Revenue
FROM factorders o
JOIN dimcustomer c ON o.CustomerKey = c.CustomerKey
JOIN dimproduct p ON o.ProductKey = p.ProductKey
JOIN dimcategory cat ON p.CategoryKey = cat.CategoryKey
WHERE o.OrderStatus = 'Complete' 
GROUP BY c.CustomerSegment, p.ProductName, cat.CategoryName;

-- --------------------------------------------------------------

CREATE OR REPLACE VIEW v_customer_loyalty AS
WITH 
-- RFM
rfm_base AS (
    SELECT 
        c.CustomerKey,
        c.CustomerSegment,
		MIN(o.OrderDate) AS FirstOrderDate,
        MAX(o.OrderDate) AS LastOrderDate,
        COUNT(DISTINCT o.OrderId) AS Frequency,
        SUM(o.SalesPerCustomer) AS Monetary,
       DATEDIFF((SELECT MAX(OrderDate) FROM factorders), MAX(o.OrderDate)) AS Recency
    FROM factorders o
    JOIN dimcustomer c ON o.CustomerKey = c.CustomerKey
    WHERE o.OrderStatus = 'Complete'
    GROUP BY c.CustomerKey, c.CustomerSegment
),

-- statistics
rfm_stats AS (
    SELECT 
        AVG(Recency) AS mean_rec, STDDEV(Recency) AS sd_rec,
        AVG(Frequency) AS mean_freq, STDDEV(Frequency) AS sd_freq,
        AVG(Monetary) AS mean_mon, STDDEV(Monetary) AS sd_mon
    FROM rfm_base
),

-- score
rfm_scored AS (
    SELECT 
        b.CustomerKey,
        b.CustomerSegment,
        b.Recency, b.Frequency, b.Monetary,b.FirstOrderDate, b.LastOrderDate,
        
        CASE 
            WHEN b.Recency <= s.mean_rec - s.sd_rec THEN 3
            WHEN b.Recency BETWEEN s.mean_rec - s.sd_rec AND s.mean_rec + s.sd_rec THEN 2
            ELSE 1
        END AS R_Score,
        
        CASE 
            WHEN b.Frequency >= s.mean_freq + s.sd_freq THEN 3
            WHEN b.Frequency BETWEEN s.mean_freq - s.sd_freq AND s.mean_freq + s.sd_freq THEN 2
            ELSE 1
        END AS F_Score,
        
        CASE 
            WHEN b.Monetary >= s.mean_mon + s.sd_mon THEN 3
            WHEN b.Monetary BETWEEN s.mean_mon - s.sd_mon AND s.mean_mon + s.sd_mon THEN 2
            ELSE 1
        END AS M_Score
    FROM rfm_base b
    CROSS JOIN rfm_stats s
)
-- total score
SELECT 
    CustomerKey,
    CustomerSegment,
    Recency, Frequency, Monetary,
    R_Score, F_Score, M_Score,
    (R_Score + F_Score + M_Score) AS RFM_Total,
    CASE 
        WHEN DATEDIFF((SELECT MAX(OrderDate) FROM factorders), FirstOrderDate) <= 30 THEN 'New Customer'
        WHEN  rfm_scored.Frequency = 1 THEN 'Low Loyal'
        WHEN (R_Score + F_Score + M_Score) >= 8 THEN 'High Loyal'
        WHEN (R_Score + F_Score + M_Score) BETWEEN 5 AND 7 THEN 'Moderate Loyal'
        ELSE 'Low Loyal'
    END AS LoyaltyLevel
FROM rfm_scored;
-- --------------------------------------------------------------------------------

SELECT 
    COUNT(DISTINCT (orderid))
FROM
    factorders
WHERE
    OrderStatus = 'complete';
-- ----------------------------------------------------------------------------
-- loyalty
CREATE OR REPLACE VIEW v_customer_loyalty_summary AS
SELECT 
    l.CustomerSegment,
    l.LoyaltyLevel,
    
     -- total customer in segment
    COUNT(DISTINCT l.CustomerKey) AS CustomerCount,
    COUNT(DISTINCT l.CustomerKey) * 100.0 
        / SUM(COUNT(DISTINCT l.CustomerKey)) OVER (PARTITION BY l.CustomerSegment)
        AS CustomerPercentage,

    -- Discout
    ROUND(AVG(o.OrderItemDiscountRate), 2) AS Avg_DiscountRate,
    ROUND(SUM(o.OrderItemDiscount), 2) AS Total_Discount_Amount,
    ROUND(SUM(o.Sales), 2) AS Total_Sales,
    ROUND(SUM(o.SalesPerCustomer), 2) AS Total_Revenue,

    -- Delivery
    COUNT(DISTINCT CASE WHEN o.DeliveryStatus = 'Shipping On Time' AND o.OrderStatus = 'Complete' THEN o.OrderId END) AS OnTimeDeliveries,
	COUNT(DISTINCT CASE WHEN o.DeliveryStatus = 'Late Delivery' AND o.OrderStatus = 'Complete' THEN o.OrderId END) AS LateDeliveries,
    COUNT(DISTINCT CASE WHEN o.DeliveryStatus = 'Advance Shipping' AND o.OrderStatus = 'Complete' THEN o.OrderId END) AS AdvanceDeliveries,
    COUNT(DISTINCT o.OrderId) AS TotalOrders,

    -- Delivery Rate
    ROUND(COUNT(CASE WHEN o.DeliveryStatus = 'Shipping On Time' THEN 1 END) * 100.0 / COUNT(*), 2) AS OnTimeRate,
    ROUND(COUNT(CASE WHEN o.DeliveryStatus = 'Late Delivery' THEN 1 END) * 100.0 / COUNT(*), 2) AS LateRate,
    ROUND(COUNT(CASE WHEN o.DeliveryStatus = 'Advance Shipping' THEN 1 END) * 100.0 / COUNT(*), 2) AS EarlyRate

FROM v_customer_loyalty l
LEFT JOIN factorders o 
    ON l.CustomerKey = o.CustomerKey
WHERE o.OrderStatus = 'Complete'
GROUP BY 
    l.CustomerSegment,
    l.LoyaltyLevel;
-- ------------------------------------------------------------    

-- ==============================================
-- sales & Profit Analysis
-- ==============================================

CREATE or replace view  v_sales_dashboard AS
SELECT 
    MONTH(f.OrderDate) AS Month,
	f.orderid,
   l.Market AS Market,
   l.City AS City,
    l.Country,
    p.ProductName,
   cat.CategoryName,
    SUM(f.Sales) AS TotalSales,
    SUM(f.SalesPerCustomer) AS RevenueAfterDiscount,
    SUM(CASE
        WHEN
            OrderProfitPerOrder > 0
                AND f.OrderStatus = 'Complete'
        THEN
            OrderProfitPerOrder
        ELSE 0
    END) AS Net_Profit,
    SUM(f.OrderProfitPerOrder) as Totalprofit,
    SUM(f.OrderItemDiscount) AS TotalDiscountAmount,
    SUM(f.OrderItemQuantity) AS TotalQuantity,
    COUNT(distinct f.OrderID) AS TotalOrders,
    COUNT(DISTINCT CASE
            WHEN f.OrderProfitPerOrder < 0 THEN f.OrderID
        END) AS NegativeProfitOrders,
    SUM(CASE
        WHEN
            f.OrderProfitPerOrder < 0
                AND f.OrderStatus = 'Complete'
        THEN
            ABS(f.OrderProfitPerOrder)
        ELSE 0
    END) AS TotalLossAmount
FROM
    factorders f
        LEFT JOIN
    dimproduct p ON f.ProductKey = p.ProductKey
      LEFT JOIN 
    dimcategory cat on p.CategoryKey=cat.CategoryKey
        LEFT JOIN
    dimlocation l ON f.LocationKey = l.LocationKey
      
WHERE
    f.OrderStatus = 'Complete'
GROUP BY Month ,f.orderid,l.Market , l.City , l.Country ,cat.CategoryName, p.productname;
-- ---------------------------------------------------------------------------------------
select *from factorders where productkey in (
select  distinct productkey  from factorders
where OrderProfitPerOrder <0 and orderstatus ='complete')and OrderProfitPerOrder >0  ;


select *from factorders where productkey in (
select  distinct locationkey  from factorders
where OrderProfitPerOrder <0 and orderstatus ='complete')and OrderProfitPerOrder >0  ;


select *from factorders where productkey in (
select  distinct shippingmodekey  from factorders
where OrderProfitPerOrder <0 and orderstatus ='complete')and OrderProfitPerOrder >0  ;

-- ------------------------------------------------------------------------------------------
