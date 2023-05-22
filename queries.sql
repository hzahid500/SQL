-- Using OVER window function determine the average price of a room
select
  neighbourhood_group,
  avg(price) OVER()
from
  `nyc_airbnb.airbnb`

-- Using window function determining the avg, min, max price of the overall datasets booking price

SELECT
  neighbourhood_group,
  avg(price) OVER() avg_price,
  min(price) OVER() min_price,
  max(price) OVER() max_price
FROM
  `nyc_airbnb.airbnb`

-- Difference from average price, using OVER window function

SELECT
  neighbourhood_group,
  price,
  ROUND(AVG(price) OVER(), 2),
  ROUND((price - AVG(price) OVER()),2) AS avg_price_diff
FROM
  `nyc_airbnb.airbnb`

-- Selecting the percent of average price with OVER() clause

SELECT
  neighbourhood_group,
  price,
  ROUND(AVG(price) OVER(), 2) AS avg_price,
  ROUND((price / AVG(price) OVER() * 100), 2) AS percentage_of_avg_price
FROM
  `nyc_airbnb.airbnb`

-- Show the percent difference from average price

SELECT
  neighbourhood_group,
  price,
  ROUND(AVG(price) OVER(),2) as avg_price,
  ROUND((price / AVG(price) OVER() - 1)* 100, 2) AS percent_diff_from_avg_price
FROM
  `nyc_airbnb.airbnb`

-- Partition by neighbourhood group

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group),2) AS avg_price_by_neigh_group
FROM
  `nyc_airbnb.airbnb`
ORDER BY
  avg_price_by_neigh_group DESC

-- PARTITION BY neighbourhood_group & neighbourhood

SELECT 
  neighbourhood_group,
  neighbourhood,
  price,
  AVG(price) OVER(PARTITION BY neighbourhood_group) AS avg_price_by_neigh_group,
  AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood) AS avg_price_by_group_and_neigh
FROM
  `nyc_airbnb.airbnb`

-- Neighbourhood group & neighbourhood & neighbourhood delta

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  AVG(price) OVER(PARTITION BY neighbourhood_group) AS avg_price_by_neigh_group,
  AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood) AS avg_price_by_group_and_neigh,
  ROUND(price - AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS neigh_group_delta,
  ROUND(price - AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood), 2) AS group_and_neigh_delta
FROM
  `nyc_airbnb.airbnb`

-- price difference by neighbourhood_group

SELECT
  neighbourhood_group,
  price,
  ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS avg_price_by_neighbourhood_group,
  ROUND(price - AVG(price) OVER(PARTITION BY neighbourhood_group),2) AS avg_price_by_neighbourhood_group_minus_price
FROM  
  `nyc_airbnb.airbnb`

-- overall price rank

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank
FROM  
  `nyc_airbnb.airbnb`

--

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  ROW_NUMBER() OVER(ORDER BY price) AS overall_price_rank_asc
FROM
  `nyc_airbnb.airbnb`

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  DENSE_RANK() OVER(PARTITION BY neighbourhood_group, neighbourhood ORDER BY price DESC) AS rank_by_neigh_group
FROM
  `nyc_airbnb.airbnb`

SELECT
  neighbourhood,
  MAX(price) AS max_price_by_neigh
FROM
  `nyc_airbnb.airbnb`
GROUP BY
  neighbourhood
ORDER BY
  max_price_by_neigh DESC

select *
from `nyc_airbnb.airbnb`

SELECT
  neighbourhood_group,
  room_type,
  price,
  ROUND(AVG(price) OVER(PARTITION BY room_type ORDER BY room_type DESC),2) as avg_price_by_room_type
FROM
  `nyc_airbnb.airbnb`

-- Flag top 3 prices by neighbourhood_group by using case statement

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
  ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
  CASE
    WHEN ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) <= 3 THEN 'Yes'
    ELSE 'No'
  END AS top3_flag
FROM
  `nyc_airbnb.airbnb`

-- RANK

SELECT 
  neighbourhood_group,
  neighbourhood,
  price,
  ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
  RANK() OVER(ORDER BY price DESC) AS overall_price_rank_with_rank,
  ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
  RANK() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank_with_rank
FROM
  `nyc_airbnb.airbnb`

-- DENSE_RANK

SELECT
  neighbourhood_group,
  neighbourhood,
  price,
  ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
  RANK() OVER(ORDER BY price DESC) AS overall_price_rank_with_rank,
  DENSE_RANK() OVER(ORDER BY price DESC) AS overall_price_rank_with_dense_rank
FROM
  `nyc_airbnb.airbnb`

-- LAG BY 1 Period

SELECT
  host_id,
  host_name,
  price,
  last_review,
  LAG(price) OVER(PARTITION BY host_name ORDER BY last_review)
FROM
  `nyc_airbnb.airbnb`

-- LAG BY 2 Periods

SELECT
  host_id,
  host_name,
  price,
  last_review,
  LAG(price, 2) OVER(PARTITION BY host_name ORDER BY last_review)
FROM
  `nyc_airbnb.airbnb`

-- Top 3 with subquery to select only the 'Yes' values in the top3_flag column

SELECT * FROM (
  SELECT
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    price,
    ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
    ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
    CASE
      WHEN ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) <= 3 THEN 'Yes'
      ELSE 'No'
  END AS top3_flag
  FROM `nyc_airbnb.airbnb`
) a
where top3_flag = 'Yes'









