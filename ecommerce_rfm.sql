-- Run this query on bigquery console
-- This query limit only transactions data in 2023
-- RFM score build using quantile method, you can leverage the RFM score calculation using more advance method like K-means clustering etc

-- Create user transaction info as base table
WITH base_tbl AS (
  SELECT u.id as user_id,
        u.age,
        u.gender,
        u.country,
        u.traffic_source as source,
        o.order_id, 
        o.num_of_item as quantity,
        DATE(o.created_at) AS created_at,
        ROUND(o.num_of_item  * p.retail_price, 2) as amount
  FROM `bigquery-public-data.thelook_ecommerce.users` u
  left join `bigquery-public-data.thelook_ecommerce.orders` o on u.id = o.user_id
  left join `bigquery-public-data.thelook_ecommerce.order_items` i on u.id = i.user_id
  left join `bigquery-public-data.thelook_ecommerce.products` p on product_id = p.id
  where i.status= 'Shipped' 
  and EXTRACT(YEAR FROM o.created_at) = 2023 --Limit data only transaction in 2023
  order by o.created_at asc
)

-- Calculate Frequency & Monetery
, fm_tbl AS (
  SELECT user_id,
        age,
        gender,
        country,
        MAX(created_at) AS recent_date,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(amount) AS monetary
  FROM base_tbl
  GROUP BY 1,2,3,4
)

-- Calculate Recency
,  rfm_tbl AS (
    SELECT 
      user_id,
      age,
      gender,
      country,
      DATE_DIFF('2024-02-20', recent_date, DAY) AS recency,  -- recency per 2024-02-20
      frequency,
      monetary,
    FROM fm_tbl
)

-- Determine quintiles for each RFM metric
,  percentile_tbl AS (
    SELECT
      rfm.*,
      --All percentiles for MONETARY
      m.percentiles[offset(20)] AS m20, 
      m.percentiles[offset(40)] AS m40,
      m.percentiles[offset(60)] AS m60, 
      m.percentiles[offset(80)] AS m80,
      m.percentiles[offset(100)] AS m100,    
      --All percentiles for FREQUENCY
      f.percentiles[offset(20)] AS f20, 
      f.percentiles[offset(40)] AS f40,
      f.percentiles[offset(60)] AS f60, 
      f.percentiles[offset(80)] AS f80,
      f.percentiles[offset(100)] AS f100,    
      --All percentiles for RECENCY
      r.percentiles[offset(20)] AS r20, 
      r.percentiles[offset(40)] AS r40,
      r.percentiles[offset(60)] AS r60, 
      r.percentiles[offset(80)] AS r80,
      r.percentiles[offset(100)] AS r100
    FROM 
      rfm_tbl rfm,
      (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM
      rfm_tbl) m,
      (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM
      rfm_tbl) f,
      (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM
      rfm_tbl) r
  )

-- Assign scores for each RFM metric
,  rfm_score AS (
    SELECT *, 
    CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score
    FROM (
        SELECT *, 
        CASE WHEN monetary <= m20 THEN 1
            WHEN monetary <= m40 AND monetary > m20 THEN 2 
            WHEN monetary <= m60 AND monetary > m40 THEN 3 
            WHEN monetary <= m80 AND monetary > m60 THEN 4 
            WHEN monetary <= m100 AND monetary > m80 THEN 5
        END AS m_score,
        CASE WHEN frequency <= f20 THEN 1
            WHEN frequency <= f40 AND frequency > f20 THEN 2 
            WHEN frequency <= f60 AND frequency > f40 THEN 3 
            WHEN frequency <= f80 AND frequency > f60 THEN 4 
            WHEN frequency <= f100 AND frequency > f80 THEN 5
        END AS f_score,
        --Recency scoring is reversed
        CASE WHEN recency <= r20 THEN 5
            WHEN recency <= r40 AND recency > r20 THEN 4 
            WHEN recency <= r60 AND recency > r40 THEN 3 
            WHEN recency <= r80 AND recency > r60 THEN 2 
            WHEN recency <= r100 AND recency > r80 THEN 1
        END AS r_score,
        FROM percentile_tbl
        )
  )

-- Define the RFM segments using the scores obtained from rfm_score cte
,  rfm_segment AS (
    SELECT
      user_id,
      age,
      gender,
      country,
      recency,
      frequency,
      monetary,
      r_score,
      f_score,
      m_score,
      fm_score,
      CASE WHEN (r_score = 5 AND fm_score = 5) 
              OR (r_score = 5 AND fm_score = 4) 
              OR (r_score = 4 AND fm_score = 5) 
          THEN 'Champions'
          WHEN (r_score = 5 AND fm_score =3) 
              OR (r_score = 4 AND fm_score = 4)
              OR (r_score = 3 AND fm_score = 5)
              OR (r_score = 3 AND fm_score = 4)
          THEN 'Loyal Customers'
          WHEN (r_score = 5 AND fm_score = 2) 
              OR (r_score = 4 AND fm_score = 2)
              OR (r_score = 3 AND fm_score = 3)
              OR (r_score = 4 AND fm_score = 3)
          THEN 'Potential Loyalists'
          WHEN r_score = 5 AND fm_score = 1 THEN 'Recent Customers'
          WHEN (r_score = 4 AND fm_score = 1) 
              OR (r_score = 3 AND fm_score = 1)
          THEN 'Promising'
          WHEN (r_score = 3 AND fm_score = 2) 
              OR (r_score = 2 AND fm_score = 3)
              OR (r_score = 2 AND fm_score = 2)
          THEN 'Customers Needing Attention'
          WHEN r_score = 2 AND fm_score = 1 THEN 'About to Sleep'
          WHEN (r_score = 2 AND fm_score = 5) 
              OR (r_score = 2 AND fm_score = 4)
              OR (r_score = 1 AND fm_score = 3)
          THEN 'At Risk'
          WHEN (r_score = 1 AND fm_score = 5)
              OR (r_score = 1 AND fm_score = 4)        
          THEN 'Cant Lose Them'
          WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'
          WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
          END AS rfm_segment 
    FROM rfm_score
  )

SELECT * FROM rfm_segment;
