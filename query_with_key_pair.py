import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

use_passphrase = os.getenv('USE_PASSPHRASE', 'false').lower() == 'true'

print("🔐 Using passphrase:", use_passphrase)

if use_passphrase:
    passphrase = os.getenv('SNOWFLAKE_KEY_PASSPHRASE')
    if not passphrase:
        raise ValueError("Missing passphrase for encrypted key!")

    with open("key.pem", "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode(),
            backend=default_backend()
        )

    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        private_key=pkb,
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
else:
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        private_key_file='key.pem',
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
print("✅ Snowflake version:", cur.fetchone()[0])
cur.close()
conn.close()
