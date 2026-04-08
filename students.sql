--data quality rules
--source 
CREATE OR REPLACE TABLE students (
    id INT,
    age INT,
    male STRING,
    female STRING
);
--target table
CREATE OR REPLACE TABLE tg_table (
    id INT,
    age INT,
    male STRING,
    female STRING
);
--error table
CREATE OR REPLACE TABLE students_error (
    id INT,
    age INT,
    male STRING,
    female STRING,
    error_msg STRING
);


INSERT INTO students VALUES
(1, 20, 'arjun', 'lakshmi'),
(2, 21, 'rahul', 'kavya'),
(3, 220, 'kiran', 'divya'),
(4, 23, 'manoj', 'pooja'),
(5, 24, 'sai', 'anitha'),

(NULL, 25, 'test', 'abc'),   -- ❌ invalid id
(7, 200, 'test', 'abc'),     -- ❌ invalid age
(8, 30, NULL, 'abc'),        -- ❌ male null
(9, 30, 'test', NULL);       -- ❌ female null


CREATE OR REPLACE PROCEDURE students_proc()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session):

    # ✅ Insert VALID data with datatype casting
    session.sql("""
        INSERT INTO tg_table
        SELECT 
            TRY_CAST(id AS INT) AS id,
            TRY_CAST(age AS INT) AS age,
            CAST(male AS STRING) AS male,
            CAST(female AS STRING) AS female
        FROM students s
        WHERE 
            TRY_CAST(id AS INT) IS NOT NULL
            AND TRY_CAST(age AS INT) BETWEEN 18 AND 60
            AND male IS NOT NULL
            AND female IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM tg_table t 
                WHERE t.id = TRY_CAST(s.id AS INT)
            )
    """).collect()


    # ❌ Insert INVALID data with reason (including datatype errors)
    session.sql("""
        INSERT INTO students_error
        SELECT 
            id,
            age,
            male,
            female,
            CASE
                WHEN TRY_CAST(id AS INT) IS NULL THEN 'INVALID ID TYPE'
                WHEN TRY_CAST(age AS INT) IS NULL THEN 'INVALID AGE TYPE'
                WHEN TRY_CAST(age AS INT) NOT BETWEEN 18 AND 60 THEN 'INVALID AGE RANGE'
                WHEN male IS NULL THEN 'MALE IS NULL'
                WHEN female IS NULL THEN 'FEMALE IS NULL'
                ELSE 'UNKNOWN ERROR'
            END AS error_reason
        FROM students
        WHERE 
            TRY_CAST(id AS INT) IS NULL
            OR TRY_CAST(age AS INT) IS NULL
            OR TRY_CAST(age AS INT) NOT BETWEEN 18 AND 60
            OR male IS NULL
            OR female IS NULL
    """).collect()

    return "DQ + Casting Applied Successfully"

$$;
CALL students_proc();

SELECT * FROM tg_table;

SELECT * FROM students_error;
