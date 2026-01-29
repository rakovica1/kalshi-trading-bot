import os
import tempfile
from pathlib import Path

import yaml

HOSTS = {
    "demo": "https://demo-api.kalshi.co/trade-api/v2",
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
}

DEFAULT_CONFIG_PATH = Path("config.yaml")


def load_config_from_env():
    """Load configuration from environment variables (for Railway / cloud deploy).

    Expected env vars:
        KALSHI_ENV          - "prod" or "demo" (default: "prod")
        KALSHI_API_KEY_ID   - API key UUID
        KALSHI_PRIVATE_KEY  - Full PEM file contents
    """
    env = os.environ.get("KALSHI_ENV", "prod")
    key_id = os.environ.get("KALSHI_API_KEY_ID")
    private_key = os.environ.get("KALSHI_PRIVATE_KEY")

    if not key_id or not private_key:
        return None

    if env not in HOSTS:
        raise ValueError(f"KALSHI_ENV must be 'demo' or 'prod', got '{env}'")

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    tmp.write(private_key)
    tmp.close()

    return {
        "host": HOSTS[env],
        "environment": env,
        "api_key_id": key_id,
        "private_key_path": tmp.name,
    }


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Load configuration from YAML file, falling back to environment variables."""
    if not path.exists():
        # Try environment variables
        env_cfg = load_config_from_env()
        if env_cfg:
            return env_cfg
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            "Copy config.example.yaml to config.yaml and fill in your credentials,\n"
            "or set KALSHI_ENV, KALSHI_API_KEY_ID, and KALSHI_PRIVATE_KEY env vars."
        )

    with open(path) as f:
        cfg = yaml.safe_load(f)

    env = cfg.get("environment", "demo")
    if env not in HOSTS:
        raise ValueError(f"environment must be 'demo' or 'prod', got '{env}'")

    api_key_id = cfg.get("api_key_id")
    if not api_key_id or api_key_id == "your-api-key-id":
        raise ValueError("Set a valid api_key_id in config.yaml")

    key_path = Path(cfg.get("private_key_path", ""))
    if not key_path.exists():
        raise FileNotFoundError(f"Private key not found: {key_path}")

    return {
        "host": HOSTS[env],
        "environment": env,
        "api_key_id": api_key_id,
        "private_key_path": str(key_path),
    }
