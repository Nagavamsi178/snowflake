import snowflake.connector
import os

print("🔌 Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cursor = conn.cursor()
print("✅ Connected successfully")

def run_sql_file(file_path):
    print(f"\n📂 Running file: {file_path}")
    with open(file_path, "r") as f:
        sql_script = f.read()
    cursor.execute(sql_script)

# Run SQL files
run_sql_file("students.sql")
run_sql_file("orders.sql")

print("🎉 Done!")

cursor.close()
conn.close()
