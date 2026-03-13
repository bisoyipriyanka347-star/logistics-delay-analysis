CREATE DATABASE logistics;
USE logistics;
CREATE TABLE logistics_data (
    event_time DATETIME,
    asset_id VARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    inventory_level INT,
    shipment_status VARCHAR(30),
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    traffic_status VARCHAR(20),
    waiting_time INT,
    user_transaction_amount DECIMAL(10,2),
    user_purchase_frequency INT,
    logistics_delay_reason VARCHAR(100),
    asset_utilization DECIMAL(5,2),
    demand_forecast INT,
    logistics_delay TINYINT
);

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/logistics_data.csv'
INTO TABLE logistics_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM logistics_data;
SELECT * FROM logistics_data;

SELECT
  SUM(event_time IS NULL) AS null_time,
  SUM(asset_id IS NULL) AS null_asset,
  SUM(shipment_status IS NULL) AS null_status,
  SUM(logistics_delay IS NULL) AS null_delay
FROM logistics_data;

SELECT logistics_delay, COUNT(*)
FROM logistics_data
GROUP BY logistics_delay;

SELECT asset_id, event_time, COUNT(*)
FROM logistics_data
GROUP BY asset_id, event_time
HAVING COUNT(*) > 1;

SELECT
  MIN(waiting_time), MAX(waiting_time),
  MIN(temperature), MAX(temperature),
  MIN(humidity), MAX(humidity)
FROM logistics_data;

SELECT shipment_status, COUNT(*)
FROM logistics_data
GROUP BY shipment_status;

UPDATE logistics_data
SET logistics_delay_reason = 'Not Specified'
WHERE logistics_delay_reason IS NULL;

UPDATE logistics_data
SET waiting_time = 0
WHERE waiting_time IS NULL;

SELECT DISTINCT traffic_status FROM logistics_data;
SELECT DISTINCT shipment_status FROM logistics_data;
SELECT DISTINCT logistics_delay_reason FROM logistics_data;

ALTER TABLE logistics_data
ADD COLUMN event_date DATE,
ADD COLUMN event_hour INT;

UPDATE logistics_data
SET
  event_date = DATE(event_time),
  event_hour = HOUR(event_time);
  
  ALTER TABLE logistics_data
ADD COLUMN delay_flag VARCHAR(10);

UPDATE logistics_data
SET delay_flag =
  CASE
    WHEN logistics_delay = 1 THEN 'Delayed'
    ELSE 'On Time'
  END;
  
SELECT *
FROM logistics_data
WHERE logistics_delay = 0
AND logistics_delay_reason <> 'Not Specified';

-- 1 Total orders & delayed orders
SELECT COUNT(*) AS total_orders,
       SUM(logistics_delay) AS delayed_orders,
	   ROUND(SUM(logistics_delay)*100.0/COUNT(*),2) AS delay_percentage 
FROM logistics_data;

-- 2 Delay % by date
SELECT 
    event_date,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY event_date
ORDER BY event_date;

-- 3️ Delay % by hour
SELECT 
    event_hour,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY event_hour
ORDER BY event_hour;

-- 4 Delay % by traffic status
SELECT 
    traffic_status,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY traffic_status
ORDER BY delay_pct DESC;

-- 5 Avg waiting time — delayed vs on-time
SELECT 
    delay_flag,
    ROUND(AVG(waiting_time), 2) AS avg_waiting_time
FROM logistics_data
GROUP BY delay_flag;

-- 6 Top delay reasons
SELECT 
    logistics_delay_reason,
    COUNT(*) AS occurrences
FROM logistics_data
WHERE logistics_delay = 1
GROUP BY logistics_delay_reason
ORDER BY occurrences DESC;

-- 7 Are delays improving or worsening compared to the previous day?
WITH daily_delay AS (
    SELECT 
        DATE(event_time) AS order_date,
        COUNT(*) AS total_orders,
        SUM(logistics_delay) AS delayed_orders,
        ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
    FROM logistics_data
    GROUP BY DATE(order_date)
)
SELECT
      order_date,
      delay_pct,
      LAG(delay_pct) OVER(ORDER BY order_date) AS pre_day_delay_pct,
      delay_pct-LAG(delay_pct) OVER(ORDER BY order_date) AS change_in_delay 
FROM daily_delay;

-- 8 Which assets contribute most to delays?
 SELECT 
        asset_id,
        COUNT(*) AS total_orders,
        SUM(logistics_delay) AS delayed_orders
    FROM logistics_data
    GROUP BY asset_id
    ORDER BY delayed_orders DESC;
    
    WITH asset_delay AS (
    SELECT 
        asset_id,
        COUNT(*) AS total_orders,
        SUM(logistics_delay) AS delayed_orders
    FROM logistics_data
    GROUP BY asset_id
)
SELECT 
    asset_id,
    total_orders,
    delayed_orders,
    ROUND(delayed_orders * 100.0 / total_orders, 2) AS delay_pct,
    RANK() OVER (ORDER BY delayed_orders DESC) AS delay_rank
FROM asset_delay;

-- 9 Which delay reasons contribute most overall?
SELECT 
    logistics_delay_reason,
    COUNT(*) AS delayed_orders,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS contribution_pct
FROM logistics_data
WHERE logistics_delay = 1
GROUP BY logistics_delay_reason
ORDER BY contribution_pct DESC;

SELECT 
    logistics_delay_reason,
    COUNT(*) AS delayed_orders
FROM logistics_data
WHERE logistics_delay = 1
GROUP BY logistics_delay_reason
ORDER BY delayed_orders DESC;

-- 10 How do you know weather is the main driver and not coincidence?
SELECT
    logistics_delay_reason,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY logistics_delay_reason
ORDER BY delay_pct DESC;

-- View: Daily Delay Trend
CREATE VIEW vw_daily_delay_trend AS
SELECT
    DATE(event_date) AS order_date,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY DATE(event_date);

-- View: Delay Reason Summary
CREATE VIEW vw_delay_reason_summary AS
SELECT
    logistics_delay_reason,
    COUNT(*) AS delayed_orders,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS contribution_pct
FROM logistics_data
WHERE logistics_delay = 1
GROUP BY logistics_delay_reason;

-- View: Asset Performance
CREATE VIEW vw_asset_performance AS
SELECT
    asset_id,
    COUNT(*) AS total_orders,
    SUM(logistics_delay) AS delayed_orders,
    ROUND(SUM(logistics_delay) * 100.0 / COUNT(*), 2) AS delay_pct
FROM logistics_data
GROUP BY asset_id;

SELECT * FROM vw_daily_delay_trend;
SELECT * FROM vw_delay_reason_summary;
SELECT * FROM vw_asset_performance;