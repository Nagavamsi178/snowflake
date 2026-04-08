orders
CREATE OR REPLACE TABLE orders (
   order_id INT,
   product_name STRING,
   amount string,
   status STRING
);

INSERT INTO orders VALUES
(101, 'TV', 50000, 'SUCCESS'),
(102, 'Mobile', 20000, 'SUCCESS'),
(103, 'Laptop', 70000, 'FAILED'),
(104, 'AC', 30000, 'SUCCESS'),
(105, 'Fridge', 40000, 'FAILED');



INSERT INTO orders VALUES
(NULL, 'TV', 50000, 'SUCCESS'),        
(106, NULL, 20000, 'SUCCESS'),         
(107, 'Laptop', -5000, 'SUCCESS'),     
(108, 'AC', 0, 'SUCCESS'),             
(109, 'Fridge', 40000, 'DONE'),        
(110, 'TV', -456, 'SUCCESS');         

CREATE OR REPLACE TABLE target_table(
   order_id INT,
   product_name STRING,
   amount INT,
   status STRING
);

--error table
CREATE OR REPLACE TABLE error_table (
   order_id STRING,
   product_name STRING,
   amount STRING,
   status STRING,
   error_msg STRING
);

CREATE OR REPLACE PROCEDURE orders_proc()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session):

    # 🔁 Optional: clear old data (avoid duplicates)
    session.sql("DELETE FROM target_table").collect()
    session.sql("DELETE FROM error_table").collect()

    # 1️⃣ VALID DATA
    session.sql("""
        INSERT INTO target_table
        SELECT 
            TRY_CAST(order_id AS INT) AS order_id,            --try cast= is a convert valid or invalid safe datatype conversion
            product_name,
            TRY_CAST(amount AS INT) AS amount,
            UPPER(status) AS status
        FROM orders o
        WHERE 
            TRY_CAST(order_id AS INT) IS NOT NULL
            AND product_name IS NOT NULL
            AND TRY_CAST(amount AS INT) IS NOT NULL
            AND TRY_CAST(amount AS INT) > 0
            AND UPPER(status) IN ('SUCCESS','FAILED')
    """).collect()


    # 2️⃣ INVALID DATA
    session.sql("""
        INSERT INTO error_table
        SELECT 
            order_id,
            product_name,
            amount,
            status,
            CASE
                WHEN TRY_CAST(order_id AS INT) IS NULL THEN 'INVALID ORDER_ID'
                WHEN product_name IS NULL THEN 'PRODUCT NAME NULL'
                WHEN TRY_CAST(amount AS INT) IS NULL THEN 'INVALID AMOUNT TYPE'
                WHEN TRY_CAST(amount AS INT) <= 0 THEN 'INVALID AMOUNT VALUE'
                WHEN UPPER(status) NOT IN ('SUCCESS','FAILED') THEN 'INVALID STATUS'
                ELSE 'UNKNOWN ERROR'
            END
        FROM orders
        WHERE 
            TRY_CAST(order_id AS INT) IS NULL
            OR product_name IS NULL
            OR TRY_CAST(amount AS INT) IS NULL
            OR TRY_CAST(amount AS INT) <= 0
            OR UPPER(status) NOT IN ('SUCCESS','FAILED')
    """).collect()

    return "DQ + Casting Applied Successfully"

$$;
CALL orders_proc();


SELECT * FROM error_table;
SELECT * FROM target_table;