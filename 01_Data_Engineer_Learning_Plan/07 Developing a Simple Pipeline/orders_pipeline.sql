-- 1. Create a bronze streaming table from our volume. 
CREATE OR REFRESH STREAMING TABLE workspace.data_engineering_labs_00.bronze_demo
AS
SELECT 
*, 
current_timestamp() AS processing_time,
_metadata.file_name AS source_file
FROM 
STREAM read_files(
  "${source}/orders", -- source config variable set in pipeline settings
  format => 'JSON'
);


-- 2. Create a silver streaming table from our bronze table, with a transform to convert the timestamp
CREATE OR REFRESH STREAMING TABLE workspace.data_engineering_labs_00.silver_demo
AS
SELECT 
order_id,
timestamp(order_timestamp) AS order_timestamp,
customer_id,
notifications
FROM 
STREAM bronze_demo;

-- 3. Create a materialised view from our silver table
CREATE OR REFRESH MATERIALIZED VIEW workspace.data_engineering_labs_00.gold_orders_by_date_demo
AS
SELECT 
date(order_timestamp) AS order_date,
count(*) AS total_daily_orders
FROM 
silver_demo
GROUP BY date(order_timestamp);



