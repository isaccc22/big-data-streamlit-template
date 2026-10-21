"""Central configuration. Every value can be overridden with an environment
variable or a `.env` file (see `.env.example`)."""

import os

try:  # optional dependency
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


# --- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "streaming-data")

# --- MongoDB ---------------------------------------------------------------
MONGO_URI = _env("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = _env("MONGO_DB", "bigdata")
MONGO_READINGS = _env("MONGO_READINGS", "readings")        # raw stream
MONGO_AGGREGATES = _env("MONGO_AGGREGATES", "aggregates")  # Spark windows

# --- Producer --------------------------------------------------------------
# Comma-separated list of adapters from sources.py: crypto, iss, airquality, earthquake
DATA_SOURCE = _env("DATA_SOURCE", "crypto,iss")
POLL_INTERVAL = float(_env("POLL_INTERVAL", "15"))  # seconds between API calls
COINGECKO_API_KEY = _env("COINGECKO_API_KEY", "")   # optional (free demo key)

# --- Spark -----------------------------------------------------------------
SPARK_WINDOW = _env("SPARK_WINDOW", "1 minute")
SPARK_WATERMARK = _env("SPARK_WATERMARK", "2 minutes")
SPARK_CHECKPOINT = _env("SPARK_CHECKPOINT", "./.checkpoints")
SPARK_STARTING_OFFSETS = _env("SPARK_STARTING_OFFSETS", "latest")

# --- Dashboard -------------------------------------------------------------
DISPLAY_TZ = _env("DISPLAY_TZ", "Asia/Manila")
DEFAULT_REFRESH_SECONDS = int(_env("REFRESH_SECONDS", "15"))
