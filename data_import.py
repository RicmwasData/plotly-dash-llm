import pandas as pd
from google.cloud import bigquery
from AI_langchain import run_query

client = bigquery.Client()


# Queries 

#sales by city
Query1= """
SELECT
  EXTRACT(YEAR FROM date) AS year,
  city,
  SUM(sale_dollars) AS total_sales
FROM
  `bigquery-public-data.iowa_liquor_sales.sales`
GROUP BY
  year,
  city
ORDER BY
  year,
  total_sales DESC
"""


# sales by categoty 
Query2="""
SELECT
  EXTRACT(YEAR FROM date) AS year,
  category_name,
  SUM(sale_dollars) AS total_sales
FROM
  `bigquery-public-data.iowa_liquor_sales.sales`
GROUP BY
  year,
  category_name
ORDER BY
  year,
  total_sales DESC
"""

#Top 5 store in each city grouped by year
# query 3
Query3="""
WITH yearly_store_sales AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year,
    city,
    store_name,
    SUM(sale_dollars) AS total_sales
  FROM
    `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY
    EXTRACT(YEAR FROM date),
    city,
    store_name
),
ranked_stores AS (
  SELECT
    year,
    city,
    store_name,
    total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY year, city
      ORDER BY total_sales DESC
    ) AS rank
  FROM
    yearly_store_sales
)
SELECT
  year,
  city,
  store_name,
  total_sales
FROM
  ranked_stores
WHERE
  rank <= 5
ORDER BY
  year,
  city,
  total_sales DESC
"""

#Sales by county and year
Query4= """
SELECT
  EXTRACT(YEAR FROM date) AS year,
  county,
  SUM(sale_dollars) AS total_sales
FROM
  `bigquery-public-data.iowa_liquor_sales.sales`
GROUP BY
  year,
  county
ORDER BY
  year,
  total_sales DESC
"""

#Top 5 cities in each county 
Query5="""
WITH yearly_city_sales AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year,
    county,
    city,
    SUM(sale_dollars) AS total_sales
  FROM
    `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY
    EXTRACT(YEAR FROM date),
    county,
    city
),
ranked_city_sales AS (
  SELECT
    year,
    county,
    city,
    total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY year, county
      ORDER BY total_sales DESC
    ) AS rank
  FROM
    yearly_city_sales
)
SELECT
  year,
  county,
  city,
  total_sales
FROM
  ranked_city_sales
WHERE
  rank <= 5
ORDER BY
  year,
  county,
  total_sales DESC
"""


# df_city= run_query(Query1)
# df_category= run_query(Query2)

#print(df.head())