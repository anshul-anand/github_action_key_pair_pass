import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

use_passphrase = os.getenv('USE_PASSPHRASE', 'false').strip().lower() == 'true'
print("USE_PASSPHRASE =", use_passphrase)

if use_passphrase:
    passphrase = os.getenv('SNOWFLAKE_KEY_PASSPHRASE')
    if not passphrase:
        raise ValueError("❌ Passphrase not provided for encrypted key!")

    with open('key.p8', 'rb') as f:
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
        role=os.environ.get('SNOWFLAKE_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

else:
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        private_key_file='key.p8',
        role=os.environ.get('SNOWFLAKE_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

cs = conn.cursor()
cs.execute("SELECT CURRENT_VERSION()")
print("✅ Snowflake Version:", cs.fetchone())
cs.close()
conn.close()
