import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

use_passphrase = os.getenv('USE_PASSPHRASE', 'false').strip().lower() == 'true'
print("USE_PASSPHRASE:", os.getenv("USE_PASSPHRASE"))
print("SNOWFLAKE_PASSPHRASE length:", len(os.getenv("SNOWFLAKE_PASSPHRASE") or ""))


if use_passphrase:
    passphrase = os.getenv('SNOWFLAKE_KEY_PASSPHRASE')
    print("USE_PASSPHRASE:", os.getenv("USE_PASSPHRASE"))
    print("SNOWFLAKE_PASSPHRASE length:", len(os.getenv("SNOWFLAKE_PASSPHRASE") or ""))

    if not passphrase:
        raise ValueError("Passphrase not provided for encrypted key!")

    with open('key.pem', 'rb') as f:
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

cs = conn.cursor()
cs.execute('SELECT CURRENT_VERSION()')
print(cs.fetchone())
cs.close()
conn.close()
