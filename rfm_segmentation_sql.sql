WITH
-- 1. Calculate the core R, F, M metrics for each customer
RFM_Metrics AS (
    SELECT
        CustomerID,
        -- Recency (R): Days since last purchase
        DATE_DIFF(
            (SELECT MAX(DATE(InvoiceDate)) FROM `psychic-mason-473812-u3.retail_analysis.transactions`),
            MAX(DATE(InvoiceDate)),
            DAY
        ) AS Recency,
        -- Frequency (F): Total number of unique invoices
        COUNT(DISTINCT InvoiceNo) AS Frequency,
        -- Monetary (M): Total spend
        SUM(TotalPrice) AS Monetary
    FROM
        `psychic-mason-473812-u3.retail_analysis.transactions`
    WHERE
        CustomerID IS NOT NULL -- Exclude rows without a CustomerID
        AND Quantity > 0      -- Exclude cancelled or returned items
        AND TotalPrice > 0    -- Exclude zero/negative revenue items
    GROUP BY
        1
),

-- 2. Assign R, F, M Scores (1-5) using NTILE
RFM_Scoring AS (
    SELECT
        *,
        -- R-Score: Lowest Recency (best) gets score 5 (6 - score)
        6 - NTILE(5) OVER (ORDER BY Recency ASC) AS R_Score,
        -- F-Score: Highest Frequency (best) gets score 5
        NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
        -- M-Score: Highest Monetary (best) gets score 5
        NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
    FROM
        RFM_Metrics
),

-- 3. Assign the Customer Segment
RFM_Segmentation AS (
    SELECT
        *,
        -- Combine scores into a single RFM_Score (e.g., '555')
        CAST(R_Score AS STRING) || CAST(F_Score AS STRING) || CAST(M_Score AS STRING) AS RFM_Score,
        CASE
            -- Champions: R >= 4 and F >= 4 (Best customers)
            WHEN R_Score >= 4 AND F_Score >= 4 THEN 'Champions'
            -- Loyal Customers: R >= 3 and F >= 3 and M >= 3 (Consistent and good value)
            WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Loyal Customers'
            -- Potential Loyalist: R >= 4 and M >= 4 (Recent and high value)
            WHEN R_Score >= 4 AND M_Score >= 4 THEN 'Potential Loyalist'
            -- New Customers: R >= 4 and F < 4 and M < 4 (Recently acquired)
            WHEN R_Score >= 4 THEN 'New Customers'
            -- Big Spenders: F >= 4 and M >= 4 and R < 4 (High value, but not recent)
            WHEN F_Score >= 4 AND M_Score >= 4 THEN 'Big Spenders'
            -- Lost Customers: R <= 2 and F <= 2 and M <= 2 (Worst segment)
            WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Lost Customers'
            -- Others: Catch-all for average/mixed segments
            ELSE 'Others'
        END AS Segment
    FROM
        RFM_Scoring
)

-- Final Query 1: RFM and Segmentation Results
SELECT
    *
FROM
    RFM_Segmentation
-- ORDER BY
--    Monetary DESC -- Example: Order by total spend