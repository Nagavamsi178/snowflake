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


def split_sql_statements(sql):
    statements = []
    current = []
    in_procedure = False

    for line in sql.splitlines():
        line_strip = line.strip()

        # Detect start/end of procedure block
        if "$$" in line_strip:
            in_procedure = not in_procedure

        current.append(line)

        # Split only if NOT inside procedure
        if ";" in line_strip and not in_procedure:
            statements.append("\n".join(current))
            current = []

    # Add remaining
    if current:
        statements.append("\n".join(current))

    return statements


def run_sql_file(file_path):
    print(f"\n📂 Running file: {file_path}")

    with open(file_path, "r") as f:
        sql_script = f.read()

    statements = split_sql_statements(sql_script)

    for i, stmt in enumerate(statements, start=1):
        stmt = stmt.strip()
        if not stmt:
            continue

        try:
            print(f"➡️ Executing statement {i}...")
            cursor.execute(stmt)
        except Exception as e:
            print(f"❌ Error in statement {i}: {e}")
            raise e

    print(f"✅ Completed: {file_path}")


# Run SQL files in order
run_sql_file("students.sql")
run_sql_file("orders.sql")

print("\n🎉 Pipeline executed successfully!")

cursor.close()
conn.close()
