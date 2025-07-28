{{ config(materialized='table') }}

WITH last_purchase AS (
    SELECT MAX(invoicedate) AS max_date FROM {{source('Customer_Data','customer_data_2')}}
),
rfm AS (
    SELECT
        customerid,
        DATE_PART('day', (SELECT max_date FROM last_purchase) - MAX(invoicedate)) AS recency,
        COUNT(DISTINCT invoiceno) AS frequency,
        SUM(total_price) AS monetary
    FROM {{source('Customer_Data','customer_data_2') }}
    GROUP BY customerid
)
SELECT * FROM rfm
