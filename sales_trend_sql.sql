CREATE TABLE `projrct_id.dataset.sales_trend` AS
WITH
-- 4. Calculate Daily Sales Trend
Daily_Sales_Trend AS (
    SELECT
        DATE(InvoiceDate) AS SaleDate,
        SUM(TotalPrice) AS DailySales
    FROM
        `project_id.dataset.transactions`
    WHERE
        Quantity > 0 AND TotalPrice > 0
    GROUP BY
        1
)

-- Final Query 2: Buying Trend Results
SELECT
    *
FROM
    Daily_Sales_Trend
ORDER BY
    SaleDate ASC
