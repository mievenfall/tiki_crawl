import os

from dotenv import load_dotenv


# Load local environment variables from .env when present.
load_dotenv()


# API ENDPOINTS
WORKER_URLS = [
    os.getenv("WORKER_URL_1"),
    os.getenv("WORKER_URL_2"),
    os.getenv("WORKER_URL_3"),
    os.getenv("WORKER_URL_4"),
    os.getenv("WORKER_URL_5"),
]

# Ignore blank/unset endpoint entries.
WORKER_URLS = [
    url.strip()
    for url in WORKER_URLS
    if url and url.strip()
]

if not WORKER_URLS:
    raise RuntimeError(
        "No worker endpoints configured. Copy .env.example to .env "
        "and set at least WORKER_URL_1."
    )


# INPUT / OUTPUT
INPUT_FILE = os.getenv("INPUT_FILE", "products_id.csv")

TEST_LIMIT_ENV = os.getenv("TEST_LIMIT")

if (
    TEST_LIMIT_ENV is None
    or TEST_LIMIT_ENV.strip().lower() in ("", "none", "full")
):
    TEST_LIMIT = None
else:
    TEST_LIMIT = int(TEST_LIMIT_ENV)

PRODUCTS_PER_FILE = int(os.getenv("PRODUCTS_PER_FILE", "1000"))

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
ERROR_DIR = os.getenv("ERROR_DIR", "errors")
STATE_DIR = os.getenv("STATE_DIR", "state")

DATABASE_FILE = os.path.join(STATE_DIR, "crawler.db")
PERMANENT_ERROR_FILE = os.path.join(ERROR_DIR, "permanent_errors.json")
TEMPORARY_ERROR_FILE = os.path.join(ERROR_DIR, "temporary_errors.json")


# CONCURRENCY / REQUESTS
MAX_THREADS = int(os.getenv("MAX_THREADS", "5"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))


# PACING
START_INTERVAL = float(os.getenv("START_INTERVAL", "1.00"))
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "0.95"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "2.00"))

SPEED_UP_STEP = float(os.getenv("SPEED_UP_STEP", "0.05"))

SLOW_DOWN_SMALL = float(os.getenv("SLOW_DOWN_SMALL", "0.10"))
SLOW_DOWN_MEDIUM = float(os.getenv("SLOW_DOWN_MEDIUM", "0.15"))
SLOW_DOWN_HIGH = float(os.getenv("SLOW_DOWN_HIGH", "0.20"))

CLEAN_CHUNKS_TO_SPEED_UP = int(
    os.getenv("CLEAN_CHUNKS_TO_SPEED_UP", "5")
)


# CHALLENGE CONTROL
LOW_CHALLENGE_RATE = float(os.getenv("LOW_CHALLENGE_RATE", "0.03"))
MEDIUM_CHALLENGE_RATE = float(os.getenv("MEDIUM_CHALLENGE_RATE", "0.10"))
HIGH_CHALLENGE_RATE = float(os.getenv("HIGH_CHALLENGE_RATE", "0.30"))

LOW_COOLDOWN = int(os.getenv("LOW_COOLDOWN", "60"))
MEDIUM_COOLDOWN = int(os.getenv("MEDIUM_COOLDOWN", "300"))
HIGH_COOLDOWN = int(os.getenv("HIGH_COOLDOWN", "420"))


# RETRY / FALLBACK
RETRY_ROUNDS = int(os.getenv("RETRY_ROUNDS", "2"))
RETRY_WAIT = int(os.getenv("RETRY_WAIT", "60"))
RETRY_MIN_INTERVAL = float(os.getenv("RETRY_MIN_INTERVAL", "1.25"))

FALLBACK_ON = {
    "timeout",
    "connection",
    "http_502",
    "http_503",
    "http_504",
    "empty",
}
