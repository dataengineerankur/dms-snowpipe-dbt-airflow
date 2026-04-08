-- SF006 - Task suspended unexpectedly
-- Category: orchestration
-- Description: root task disabled
-- Intentional failure for PATCHIT testing.

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ANALYTICS;
USE SCHEMA PUBLIC;

-- Create a sample table for task orchestration demo
CREATE OR REPLACE TABLE task_status (
    task_id INT,
    task_name VARCHAR(100),
    status VARCHAR(50),
    execution_time TIMESTAMP_LTZ,
    result_count INT
);

-- Insert initial status records
INSERT INTO task_status (task_id, task_name, status, execution_time, result_count)
VALUES 
    (1, 'extract_customers', 'pending', CURRENT_TIMESTAMP(), 0),
    (2, 'extract_orders', 'pending', CURRENT_TIMESTAMP(), 0),
    (3, 'transform_data', 'pending', CURRENT_TIMESTAMP(), 0);

-- Create root task that checks data freshness
CREATE OR REPLACE TASK root_data_check
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
AS
BEGIN
    -- Check if new data arrived
    LET row_count INT;
    
    SELECT COUNT(*)
    INTO :row_count
    FROM raw_data.public.staging_table
    WHERE load_timestamp > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP());
    
    -- Update task status table with check results
    UPDATE task_status SET status = 'checked', execution_time = CURRENT_TIMESTAMP(), result_count = :row_count FROM raw_data.public.staging_table WHERE task_name = 'extract_customers';
END;

-- Create child task that processes the data
CREATE OR REPLACE TASK process_new_data
    WAREHOUSE = COMPUTE_WH
    AFTER root_data_check
AS
BEGIN
    -- Process new records
    INSERT INTO processed_data
    SELECT 
        customer_id,
        customer_name,
        order_date,
        total_amount
    FROM raw_data.public.staging_table
    WHERE load_timestamp > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP());
    
    -- Log completion
    UPDATE task_status
    SET status = 'completed',
        execution_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'transform_data';
END;

-- Resume tasks to enable execution
ALTER TASK process_new_data RESUME;
ALTER TASK root_data_check RESUME;
