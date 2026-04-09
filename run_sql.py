import snowflake.connector
import os
from cryptography.hazmat.primitives import serialization

print("🔐 Loading private key...")

# ✅ Fix GitHub newline issue
private_key_str = os.environ["SNOWFLAKE_PRIVATE_KEY"].replace("\\n", "\n")

private_key = serialization.load_pem_private_key(
    private_key_str.encode(),
    password=None
)

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

print("🔌 Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cursor = conn.cursor()
print("✅ Connected successfully (Key Auth)")
