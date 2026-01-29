import base64
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

    # Handle base64-encoded PEM (no newline issues)
    if not private_key.startswith("-----"):
        try:
            private_key = base64.b64decode(private_key).decode("utf-8")
        except Exception:
            pass

    # Fix newlines that Railway may have stripped:
    # "-----BEGIN ... KEY-----MIIEv..." -> proper PEM with line breaks
    if "-----BEGIN" in private_key and "\n" not in private_key:
        # Newlines were stripped — reconstruct proper PEM
        private_key = private_key.replace("-----BEGIN ", "\n-----BEGIN ")
        private_key = private_key.replace("-----END ", "\n-----END ")
        private_key = private_key.replace("----- ", "-----\n")
        private_key = private_key.replace(" -----", "\n-----")

        # Re-wrap the base64 body to 64-char lines
        lines = private_key.strip().split("\n")
        rebuilt = [lines[0]]  # header
        body = "".join(lines[1:-1])  # join all base64 content
        for i in range(0, len(body), 64):
            rebuilt.append(body[i:i+64])
        rebuilt.append(lines[-1])  # footer
        private_key = "\n".join(rebuilt) + "\n"

    # Also handle literal \n escape sequences from env var
    if "\\n" in private_key:
        private_key = private_key.replace("\\n", "\n")

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
