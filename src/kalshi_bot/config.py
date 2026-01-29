import base64
import logging
import os
import tempfile
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

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

    # Debug: show what Railway passed (redacted for security)
    raw = private_key
    logger.warning(f"PEM DEBUG: len={len(raw)}")
    logger.warning(f"PEM DEBUG: first 50 chars: {repr(raw[:50])}")
    logger.warning(f"PEM DEBUG: last 50 chars:  {repr(raw[-50:])}")
    logger.warning(f"PEM DEBUG: has real newlines: {'\\n' in raw}")
    logger.warning(f"PEM DEBUG: has literal backslash-n: {'\\\\n' in repr(raw)}")
    logger.warning(f"PEM DEBUG: starts with -----: {raw.startswith('-----')}")
    logger.warning(f"PEM DEBUG: newline count: {raw.count(chr(10))}")

    # Handle literal \n escape sequences from env var (check FIRST)
    if "\\n" in private_key:
        logger.warning("PEM FIX: replacing literal \\n with real newlines")
        private_key = private_key.replace("\\n", "\n")

    # Handle base64-encoded PEM (no newline issues)
    if not private_key.startswith("-----"):
        try:
            decoded = base64.b64decode(private_key).decode("utf-8")
            if "-----BEGIN" in decoded:
                logger.warning("PEM FIX: decoded from base64")
                private_key = decoded
        except Exception:
            pass

    # Fix newlines that Railway may have stripped:
    # "-----BEGIN ... KEY-----MIIEv..." -> proper PEM with line breaks
    if "-----BEGIN" in private_key and "\n" not in private_key:
        logger.warning("PEM FIX: reconstructing newlines (all on one line)")
        # Split on the header/footer markers
        import re
        match = re.match(
            r'(-----BEGIN [A-Z ]+-----)(.+)(-----END [A-Z ]+-----)',
            private_key.strip()
        )
        if match:
            header, body, footer = match.groups()
            # Remove any spaces from body
            body = body.replace(" ", "")
            # Re-wrap to 64-char lines
            lines = [header]
            for i in range(0, len(body), 64):
                lines.append(body[i:i+64])
            lines.append(footer)
            private_key = "\n".join(lines) + "\n"

    # Ensure trailing newline
    if not private_key.endswith("\n"):
        private_key += "\n"

    # Debug: show final result
    final_lines = private_key.strip().split("\n")
    logger.warning(f"PEM FINAL: {len(final_lines)} lines, first={final_lines[0]}, last={final_lines[-1]}")

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    tmp.write(private_key)
    tmp.close()
    logger.warning(f"PEM FINAL: written to {tmp.name}")

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
