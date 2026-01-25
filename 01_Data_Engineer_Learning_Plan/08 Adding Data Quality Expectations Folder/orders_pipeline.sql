-- 1. Create a bronze streaming table from our volume. 
CREATE OR REFRESH STREAMING TABLE workspace.data_engineering_labs_00.bronze_demo_expectations
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
CREATE OR REFRESH STREAMING TABLE workspace.data_engineering_labs_00.silver_demo_expectations

-- Add the expectations
(
CONSTRAINT valid_notification EXPECT (notifications IN ('Yes', 'No')), -- Check for a Y or N in notifications column
CONSTRAINT valid_date EXPECT (order_timestamp > "2022-01-01") ON VIOLATION DROP ROW, --drop row if not valid date
CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE -- Fail pipeline if null
)


AS
SELECT 
order_id,
timestamp(order_timestamp) AS order_timestamp,
customer_id,
notifications
FROM 
STREAM bronze_demo_expectations;

-- 3. Create a materialised view from our silver table
CREATE OR REFRESH MATERIALIZED VIEW workspace.data_engineering_labs_00.gold_orders_by_date_demo_expectations
AS
SELECT 
date(order_timestamp) AS order_date,
count(*) AS total_daily_orders
FROM 
silver_demo_expectations
GROUP BY date(order_timestamp);