# vault_client.py
import hvac
import os


def get_secret(path: str) -> dict:
    """
    Fetch secrets from Vault KV v2 engine.

    Args:
        path (str): Secret name inside the 'etl' mount (e.g., 'postgres')

    Returns:
        dict: Secrets as key-value pairs
    """
    VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
    VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "root")

    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)

    if not client.is_authenticated():
        raise RuntimeError("Vault authentication failed")

    read_response = client.secrets.kv.v2.read_secret_version(
        path=path, mount_point="etl"  # Important: 'etl' is your KV v2 mount
    )

    return read_response["data"]["data"]
