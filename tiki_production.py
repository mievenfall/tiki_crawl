import csv
import json
import os
import re
import sqlite3
import threading
import time
import itertools

import requests

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# CONFIG
# ============================================================

WORKER_URLS = [
    "https://project2-w1.mngoc2603.workers.dev",

    "https://project2-w2.mngoc2603.workers.dev",

    "https://project2-w3.mngoc2603.workers.dev",

    "https://project2-w4.mngoc2603.workers.dev",

    "https://project2-w5.mngoc2603.workers.dev",
]

INPUT_FILE = "products_id.csv"

# None = full 200k
TEST_LIMIT = 10000

MAX_THREADS = 5

CHUNK_SIZE = 100

REQUEST_TIMEOUT = 15

PRODUCTS_PER_FILE = 1000


# ============================================================
# GLOBAL PACING
# ============================================================

# Start from known-stable benchmark
START_INTERVAL = 0.85

# Do not go faster than this automatically
MIN_INTERVAL = 0.85

# Slowest adaptive rate
MAX_INTERVAL = 2.00

SPEED_UP_STEP = 0.05

SLOW_DOWN_SMALL = 0.15
SLOW_DOWN_MEDIUM = 0.30
SLOW_DOWN_HIGH = 0.50

CLEAN_CHUNKS_TO_SPEED_UP = 3


# ============================================================
# CHALLENGE THRESHOLDS
# ============================================================

LOW_CHALLENGE_RATE = 0.03
MEDIUM_CHALLENGE_RATE = 0.10
HIGH_CHALLENGE_RATE = 0.30

MEDIUM_COOLDOWN = 300
HIGH_COOLDOWN = 900


# ============================================================
# RETRY
# ============================================================

RETRY_ROUNDS = 2

RETRY_WAIT = 60

# Retry slower than normal crawl
RETRY_MIN_INTERVAL = 1.50


# ============================================================
# FALLBACK
# ============================================================

# Only endpoint-specific failures use immediate fallback.
#
# HTML challenge does NOT use immediate fallback.
#
# Maximum:
# original request + 1 fallback attempt

FALLBACK_ON = {
    "timeout",
    "connection",
    "http_502",
    "http_503",
    "http_504",
    "empty",
}


# ============================================================
# OUTPUT
# ============================================================

OUTPUT_DIR = "output"
ERROR_DIR = "errors"
STATE_DIR = "state"

DATABASE_FILE = os.path.join(
    STATE_DIR,
    "crawler.db"
)

PERMANENT_ERROR_FILE = os.path.join(
    ERROR_DIR,
    "permanent_errors.json"
)

TEMPORARY_ERROR_FILE = os.path.join(
    ERROR_DIR,
    "temporary_errors.json"
)


os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)

os.makedirs(
    ERROR_DIR,
    exist_ok=True
)

os.makedirs(
    STATE_DIR,
    exist_ok=True
)


# ============================================================
# HELPERS
# ============================================================

def format_time(seconds):

    seconds = int(seconds)

    hours = seconds // 3600

    minutes = (
        seconds % 3600
    ) // 60

    seconds = seconds % 60

    return (
        f"{hours:02d}:"
        f"{minutes:02d}:"
        f"{seconds:02d}"
    )


def clean_description(description):

    if not description:
        return ""

    soup = BeautifulSoup(
        description,
        "html.parser"
    )

    text = soup.get_text(
        " ",
        strip=True
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


def load_product_ids(filename):

    product_ids = []

    with open(
        filename,
        "r",
        encoding="utf-8-sig"
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            product_id = row.get("id")

            if product_id:

                product_ids.append(
                    str(product_id).strip()
                )

    # remove duplicate IDs
    return list(
        dict.fromkeys(
            product_ids
        )
    )


# ============================================================
# THREAD LOCAL SESSION
# ============================================================

thread_local = threading.local()


def get_session():

    if not hasattr(
        thread_local,
        "session"
    ):

        session = requests.Session()

        session.headers.update({

            "Accept":
                "application/json, text/plain, */*",

            "User-Agent":
                (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/153.0.0.0 "
                    "Safari/537.36"
                )
        })

        thread_local.session = session

    return thread_local.session


# ============================================================
# SHARED GLOBAL PACER
# ============================================================

class SharedPacer:

    def __init__(self, interval):

        self.interval = interval

        self.lock = threading.Lock()

        self.last_request_time = 0.0


    def wait(self):

        with self.lock:

            now = time.monotonic()

            elapsed = (
                now
                - self.last_request_time
            )

            wait_time = (
                self.interval
                - elapsed
            )

            if wait_time > 0:

                time.sleep(
                    wait_time
                )

            self.last_request_time = (
                time.monotonic()
            )


    def set_interval(self, value):

        value = max(
            MIN_INTERVAL,
            min(
                MAX_INTERVAL,
                value
            )
        )

        with self.lock:

            self.interval = value


    def get_interval(self):

        with self.lock:

            return self.interval


pacer = SharedPacer(
    START_INTERVAL
)


# ============================================================
# ENDPOINT POOL
# ============================================================

endpoint_lock = threading.Lock()

endpoint_counter = itertools.count()


def get_next_endpoint(
    exclude=None
):

    if exclude is None:
        exclude = set()

    with endpoint_lock:

        for _ in range(
            len(WORKER_URLS)
        ):

            index = (
                next(endpoint_counter)
                % len(WORKER_URLS)
            )

            endpoint = WORKER_URLS[
                index
            ]

            if endpoint not in exclude:

                return endpoint

    return None


# ============================================================
# DATABASE
# ============================================================

def connect_database():

    conn = sqlite3.connect(
        DATABASE_FILE
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS results (

            product_id TEXT PRIMARY KEY,

            status TEXT NOT NULL,

            product_json TEXT,

            reason TEXT,

            error_type TEXT,

            attempts INTEGER NOT NULL DEFAULT 1,

            endpoint TEXT,

            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (

            key TEXT PRIMARY KEY,

            value TEXT
        )
        """
    )

    conn.commit()

    return conn


def set_metadata(
    conn,
    key,
    value
):

    conn.execute(
        """
        INSERT INTO metadata (
            key,
            value
        )

        VALUES (?, ?)

        ON CONFLICT(key)

        DO UPDATE SET
            value = excluded.value
        """,
        (
            key,
            str(value)
        )
    )

    conn.commit()


def get_metadata(
    conn,
    key,
    default=None
):

    row = conn.execute(
        """
        SELECT value

        FROM metadata

        WHERE key = ?
        """,
        (
            key,
        )
    ).fetchone()

    if row is None:

        return default

    return row[0]


def save_result(
    conn,
    result
):

    product_json = None

    if result.get(
        "product"
    ) is not None:

        product_json = json.dumps(
            result[
                "product"
            ],
            ensure_ascii=False
        )


    conn.execute(
        """
        INSERT INTO results (

            product_id,
            status,
            product_json,
            reason,
            error_type,
            attempts,
            endpoint,
            updated_at
        )

        VALUES (
            ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
        )

        ON CONFLICT(product_id)

        DO UPDATE SET

            status =
                excluded.status,

            product_json =
                excluded.product_json,

            reason =
                excluded.reason,

            error_type =
                excluded.error_type,

            attempts =
                excluded.attempts,

            endpoint =
                excluded.endpoint,

            updated_at =
                CURRENT_TIMESTAMP
        """,

        (
            result[
                "product_id"
            ],

            result[
                "status"
            ],

            product_json,

            result.get(
                "reason"
            ),

            result.get(
                "error_type"
            ),

            result.get(
                "attempts",
                1
            ),

            result.get(
                "endpoint"
            )
        )
    )


# ============================================================
# SINGLE HTTP REQUEST
# ============================================================

def request_product(
    product_id,
    endpoint,
    attempt
):

    pacer.wait()

    session = get_session()

    url = (
        f"{endpoint}/"
        f"{product_id}"
    )


    try:

        response = session.get(
            url,
            timeout=REQUEST_TIMEOUT
        )


        # ----------------------------------------------------
        # 404
        # ----------------------------------------------------

        if response.status_code == 404:

            return {

                "product_id":
                    product_id,

                "status":
                    "permanent_error",

                "reason":
                    "404 Not Found",

                "error_type":
                    "404",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        # ----------------------------------------------------
        # HTTP ERRORS
        # ----------------------------------------------------

        if response.status_code != 200:

            error_type = (
                f"http_"
                f"{response.status_code}"
            )

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    (
                        f"HTTP "
                        f"{response.status_code}"
                    ),

                "error_type":
                    error_type,

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        text = response.text


        # ----------------------------------------------------
        # EMPTY RESPONSE
        # ----------------------------------------------------

        if not text.strip():

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    "Empty response",

                "error_type":
                    "empty",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        # ----------------------------------------------------
        # HTML CHALLENGE
        # ----------------------------------------------------

        content_type = (
            response.headers
            .get(
                "Content-Type",
                ""
            )
            .lower()
        )

        stripped_text = (
            text.lstrip()
            .lower()
        )


        if (
            "text/html"
            in content_type
            or
            stripped_text.startswith(
                "<!doctype html"
            )
            or
            stripped_text.startswith(
                "<html"
            )
        ):

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    "HTML challenge",

                "error_type":
                    "challenge",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        try:

            data = response.json()

        except ValueError:

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    "Invalid JSON",

                "error_type":
                    "invalid_json",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        returned_id = data.get(
            "id"
        )


        if returned_id is None:

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    "Missing product id",

                "error_type":
                    "invalid_product",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        if (
            str(returned_id)
            != str(product_id)
        ):

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    (
                        f"ID mismatch: "
                        f"{returned_id}"
                    ),

                "error_type":
                    "id_mismatch",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        # ----------------------------------------------------
        # IMAGES
        # ----------------------------------------------------

        images_url = []

        for image in data.get(
            "images",
            []
        ):

            image_url = image.get(
                "base_url"
            )

            if image_url:

                images_url.append(
                    image_url
                )


        # ----------------------------------------------------
        # PRODUCT
        # ----------------------------------------------------

        product = {

            "id":
                returned_id,

            "name":
                data.get(
                    "name"
                ),

            "url_key":
                data.get(
                    "url_key"
                ),

            "price":
                data.get(
                    "price"
                ),

            "description":
                clean_description(
                    data.get(
                        "description"
                    )
                ),

            "images_url":
                images_url
        }


        return {

            "product_id":
                product_id,

            "status":
                "success",

            "product":
                product,

            "reason":
                None,

            "error_type":
                None,

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


    except requests.exceptions.Timeout:

        return {

            "product_id":
                product_id,

            "status":
                "temporary_error",

            "reason":
                "Timeout",

            "error_type":
                "timeout",

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


    except requests.exceptions.ConnectionError as error:

        return {

            "product_id":
                product_id,

            "status":
                "temporary_error",

            "reason":
                (
                    f"ConnectionError: "
                    f"{error}"
                ),

            "error_type":
                "connection",

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


    except requests.exceptions.RequestException as error:

        return {

            "product_id":
                product_id,

            "status":
                "temporary_error",

            "reason":
                (
                    f"{type(error).__name__}: "
                    f"{error}"
                ),

            "error_type":
                "request_error",

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


# ============================================================
# FETCH WITH FALLBACK
# ============================================================

def fetch_product(
    product_id,
    attempt
):

    primary_endpoint = (
        get_next_endpoint()
    )


    first_result = request_product(
        product_id,
        primary_endpoint,
        attempt
    )


    # --------------------------------------------------------
    # SUCCESS / 404
    # --------------------------------------------------------

    if first_result[
        "status"
    ] != "temporary_error":

        return first_result


    error_type = first_result.get(
        "error_type"
    )


    # --------------------------------------------------------
    # CHALLENGE:
    # DO NOT IMMEDIATELY SWITCH ENDPOINT
    # --------------------------------------------------------

    if error_type == "challenge":

        return first_result


    # --------------------------------------------------------
    # FALLBACK ONLY FOR ENDPOINT-SPECIFIC ERRORS
    # --------------------------------------------------------

    if error_type not in FALLBACK_ON:

        return first_result


    fallback_endpoint = (
        get_next_endpoint(
            exclude={
                primary_endpoint
            }
        )
    )


    if fallback_endpoint is None:

        return first_result


    fallback_result = request_product(
        product_id,
        fallback_endpoint,
        attempt
    )


    # Mark fallback information
    fallback_result[
        "reason"
    ] = (
        fallback_result.get(
            "reason"
        )
        or
        "Recovered by fallback"
    )


    return fallback_result


# ============================================================
# RUN CHUNK
# ============================================================

def run_chunk(
    conn,
    product_ids,
    attempt
):

    stats = {

        "success": 0,

        "permanent": 0,

        "temporary": 0,

        "challenge": 0
    }


    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:


        futures = {

            executor.submit(
                fetch_product,
                product_id,
                attempt
            ):
            product_id

            for product_id
            in product_ids
        }


        for future in as_completed(
            futures
        ):

            product_id = futures[
                future
            ]


            try:

                result = (
                    future.result()
                )

            except Exception as error:

                result = {

                    "product_id":
                        product_id,

                    "status":
                        "temporary_error",

                    "reason":
                        (
                            "Unexpected worker error: "
                            f"{type(error).__name__}: "
                            f"{error}"
                        ),

                    "error_type":
                        "worker_error",

                    "attempts":
                        attempt,

                    "endpoint":
                        None
                }


            save_result(
                conn,
                result
            )


            status = result[
                "status"
            ]


            if status == "success":

                stats[
                    "success"
                ] += 1


            elif status == "permanent_error":

                stats[
                    "permanent"
                ] += 1


            else:

                stats[
                    "temporary"
                ] += 1


                if (
                    result.get(
                        "error_type"
                    )
                    == "challenge"
                ):

                    stats[
                        "challenge"
                    ] += 1


    conn.commit()

    return stats


# ============================================================
# ADAPTIVE PACING
# ============================================================

def adjust_pacing(
    conn,
    stats,
    total
):

    if total == 0:
        return


    challenge_rate = (
        stats[
            "challenge"
        ]
        / total
    )


    current_interval = (
        pacer.get_interval()
    )


    clean_streak = int(
        get_metadata(
            conn,
            "clean_streak",
            "0"
        )
    )


    # --------------------------------------------------------
    # HIGH CHALLENGE
    # --------------------------------------------------------

    if (
        challenge_rate
        >= HIGH_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_HIGH
        )

        pacer.set_interval(
            new_interval
        )

        clean_streak = 0


        print(
            f"High challenge "
            f"{challenge_rate * 100:.1f}%"
        )

        print(
            f"Interval -> "
            f"{new_interval:.2f}s"
        )

        print(
            f"Cooldown "
            f"{HIGH_COOLDOWN}s"
        )


        set_metadata(
            conn,
            "current_interval",
            new_interval
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak
        )


        time.sleep(
            HIGH_COOLDOWN
        )

        return


    # --------------------------------------------------------
    # MEDIUM
    # --------------------------------------------------------

    if (
        challenge_rate
        >= MEDIUM_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_MEDIUM
        )

        pacer.set_interval(
            new_interval
        )

        clean_streak = 0


        print(
            f"Challenge "
            f"{challenge_rate * 100:.1f}%"
        )

        print(
            f"Interval -> "
            f"{new_interval:.2f}s"
        )

        print(
            f"Cooldown "
            f"{MEDIUM_COOLDOWN}s"
        )


        set_metadata(
            conn,
            "current_interval",
            new_interval
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak
        )


        time.sleep(
            MEDIUM_COOLDOWN
        )

        return


    # --------------------------------------------------------
    # SMALL CHALLENGE
    # --------------------------------------------------------

    if (
        challenge_rate
        >= LOW_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_SMALL
        )

        pacer.set_interval(
            new_interval
        )

        clean_streak = 0


        print(
            f"Small challenge "
            f"{challenge_rate * 100:.1f}%"
        )

        print(
            f"Interval -> "
            f"{new_interval:.2f}s"
        )


        set_metadata(
            conn,
            "current_interval",
            new_interval
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak
        )

        return


    # --------------------------------------------------------
    # CLEAN
    # --------------------------------------------------------

    if (
        stats[
            "challenge"
        ]
        == 0
    ):

        clean_streak += 1


        if (
            clean_streak
            >= CLEAN_CHUNKS_TO_SPEED_UP
        ):

            new_interval = max(
                MIN_INTERVAL,
                current_interval
                - SPEED_UP_STEP
            )


            if (
                new_interval
                < current_interval
            ):

                pacer.set_interval(
                    new_interval
                )

                print(
                    f"{clean_streak} clean chunks"
                    f" -> interval "
                    f"{new_interval:.2f}s"
                )


            clean_streak = 0


    else:

        clean_streak = 0


    set_metadata(
        conn,
        "current_interval",
        pacer.get_interval()
    )

    set_metadata(
        conn,
        "clean_streak",
        clean_streak
    )


# ============================================================
# COUNTS
# ============================================================

def get_counts(conn):

    counts = {

        "success":
            0,

        "permanent_error":
            0,

        "temporary_error":
            0
    }


    rows = conn.execute(
        """
        SELECT
            status,
            COUNT(*)

        FROM results

        GROUP BY status
        """
    ).fetchall()


    for status, count in rows:

        counts[
            status
        ] = count


    return counts


# ============================================================
# PENDING IDS
# ============================================================

def get_pending_ids(
    conn,
    product_ids
):

    existing = {

        row[0]

        for row in conn.execute(
            """
            SELECT product_id
            FROM results
            """
        )
    }


    return [

        product_id

        for product_id
        in product_ids

        if product_id
        not in existing
    ]


# ============================================================
# PROGRESS
# ============================================================

def print_progress(
    conn,
    total_ids,
    start_time,
    phase
):

    counts = get_counts(
        conn
    )


    classified = (

        counts[
            "success"
        ]

        + counts[
            "permanent_error"
        ]

        + counts[
            "temporary_error"
        ]
    )


    elapsed = (
        time.time()
        - start_time
    )


    if elapsed > 0:

        rate = (
            classified
            / elapsed
            * 60
        )

    else:

        rate = 0


    remaining = max(
        0,
        total_ids
        - classified
    )


    if rate > 0:

        eta = (
            remaining
            / rate
            * 60
        )

    else:

        eta = 0


    print()
    print(
        "========================================"
    )

    print(
        phase
    )

    print(
        f"Processed: "
        f"{classified}/"
        f"{total_ids}"
    )

    print(
        f"Success: "
        f"{counts['success']}"
    )

    print(
        f"Permanent: "
        f"{counts['permanent_error']}"
    )

    print(
        f"Temporary: "
        f"{counts['temporary_error']}"
    )

    print(
        f"Global interval: "
        f"{pacer.get_interval():.2f}s"
    )

    print(
        f"Runtime: "
        f"{format_time(elapsed)}"
    )

    print(
        f"Rate: "
        f"{rate:.2f} IDs/min"
    )

    print(
        f"ETA: "
        f"{format_time(eta)}"
    )

    print(
        "========================================"
    )


# ============================================================
# FIRST PASS
# ============================================================

def first_pass(
    conn,
    product_ids,
    start_time
):

    pending_ids = get_pending_ids(
        conn,
        product_ids
    )


    print(
        f"First-pass pending: "
        f"{len(pending_ids)}"
    )


    total_chunks = (

        len(pending_ids)
        + CHUNK_SIZE
        - 1

    ) // CHUNK_SIZE


    for chunk_number in range(
        total_chunks
    ):

        start = (
            chunk_number
            * CHUNK_SIZE
        )

        end = (
            start
            + CHUNK_SIZE
        )


        chunk = pending_ids[
            start:end
        ]


        stats = run_chunk(
            conn,
            chunk,
            attempt=1
        )


        print()
        print(
            f"Chunk "
            f"{chunk_number + 1}/"
            f"{total_chunks}"
        )

        print(
            f"success="
            f"{stats['success']}"
            f" | permanent="
            f"{stats['permanent']}"
            f" | temporary="
            f"{stats['temporary']}"
            f" | challenge="
            f"{stats['challenge']}"
        )


        adjust_pacing(
            conn,
            stats,
            len(chunk)
        )


        print_progress(
            conn,
            len(product_ids),
            start_time,
            "FIRST PASS"
        )


    set_metadata(
        conn,
        "first_pass_complete",
        "1"
    )


# ============================================================
# RETRY
# ============================================================

def retry_round(
    conn,
    round_number,
    total_ids,
    start_time
):

    rows = conn.execute(
        """
        SELECT product_id

        FROM results

        WHERE
            status =
                'temporary_error'

        AND
            attempts = ?

        ORDER BY rowid
        """,
        (
            round_number,
        )
    ).fetchall()


    retry_ids = [

        row[0]

        for row in rows
    ]


    if not retry_ids:

        print(
            f"Retry round "
            f"{round_number}: "
            f"nothing to retry"
        )

        return


    print()
    print(
        f"Retry round "
        f"{round_number}"
        f" | IDs="
        f"{len(retry_ids)}"
    )


    print(
        f"Waiting "
        f"{RETRY_WAIT}s..."
    )


    time.sleep(
        RETRY_WAIT
    )


    if (
        pacer.get_interval()
        < RETRY_MIN_INTERVAL
    ):

        pacer.set_interval(
            RETRY_MIN_INTERVAL
        )


    total_chunks = (

        len(retry_ids)
        + CHUNK_SIZE
        - 1

    ) // CHUNK_SIZE


    for chunk_number in range(
        total_chunks
    ):

        start = (
            chunk_number
            * CHUNK_SIZE
        )

        end = (
            start
            + CHUNK_SIZE
        )


        chunk = retry_ids[
            start:end
        ]


        stats = run_chunk(
            conn,
            chunk,
            attempt=(
                round_number
                + 1
            )
        )


        print()
        print(
            f"Retry "
            f"{round_number}"
            f" chunk "
            f"{chunk_number + 1}/"
            f"{total_chunks}"
        )

        print(
            f"success="
            f"{stats['success']}"
            f" | permanent="
            f"{stats['permanent']}"
            f" | temporary="
            f"{stats['temporary']}"
            f" | challenge="
            f"{stats['challenge']}"
        )


        adjust_pacing(
            conn,
            stats,
            len(chunk)
        )


        print_progress(
            conn,
            total_ids,
            start_time,
            f"RETRY {round_number}"
        )


# ============================================================
# ATOMIC JSON
# ============================================================

def save_json_atomic(
    data,
    filename
):

    temp_file = (
        filename
        + ".tmp"
    )


    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2
        )


    os.replace(
        temp_file,
        filename
    )


# ============================================================
# EXPORT PRODUCTS
# ============================================================

def export_products(conn):

    for filename in os.listdir(
        OUTPUT_DIR
    ):

        if (
            filename.startswith(
                "products_"
            )
            and
            filename.endswith(
                ".json"
            )
        ):

            os.remove(
                os.path.join(
                    OUTPUT_DIR,
                    filename
                )
            )


    rows = conn.execute(
        """
        SELECT product_json

        FROM results

        WHERE status =
            'success'

        ORDER BY rowid
        """
    )


    batch = []

    batch_number = 1

    total = 0


    for row in rows:

        batch.append(
            json.loads(
                row[0]
            )
        )

        total += 1


        if (
            len(batch)
            == PRODUCTS_PER_FILE
        ):

            filename = os.path.join(
                OUTPUT_DIR,
                f"products_"
                f"{batch_number:03d}"
                f".json"
            )


            save_json_atomic(
                batch,
                filename
            )


            print(
                f"Saved "
                f"{filename}"
            )


            batch = []

            batch_number += 1


    if batch:

        filename = os.path.join(
            OUTPUT_DIR,
            f"products_"
            f"{batch_number:03d}"
            f".json"
        )


        save_json_atomic(
            batch,
            filename
        )


        print(
            f"Saved "
            f"{filename}"
        )


    return total


# ============================================================
# EXPORT ERRORS
# ============================================================

def export_errors(conn):

    permanent_rows = conn.execute(
        """
        SELECT
            product_id,
            reason,
            attempts,
            endpoint

        FROM results

        WHERE status =
            'permanent_error'

        ORDER BY rowid
        """
    ).fetchall()


    temporary_rows = conn.execute(
        """
        SELECT
            product_id,
            reason,
            attempts,
            endpoint

        FROM results

        WHERE status =
            'temporary_error'

        ORDER BY rowid
        """
    ).fetchall()


    permanent = [

        {
            "product_id": row[0],
            "reason": row[1],
            "attempts": row[2],
            "endpoint": row[3],
        }

        for row in permanent_rows
    ]


    temporary = [

        {
            "product_id": row[0],
            "reason": row[1],
            "attempts": row[2],
            "endpoint": row[3],
        }

        for row in temporary_rows
    ]


    save_json_atomic(
        permanent,
        PERMANENT_ERROR_FILE
    )


    save_json_atomic(
        temporary,
        TEMPORARY_ERROR_FILE
    )


    return (
        len(permanent),
        len(temporary)
    )


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = (
        time.time()
    )


    product_ids = load_product_ids(
        INPUT_FILE
    )


    if TEST_LIMIT is not None:

        product_ids = product_ids[
            :TEST_LIMIT
        ]


    total_ids = len(
        product_ids
    )


    print(
        "========================================"
    )

    print(
        "TIKI PRODUCTION CRAWLER"
    )

    print(
        "========================================"
    )

    print(
        f"IDs: "
        f"{total_ids}"
    )

    print(
        f"Worker endpoints: "
        f"{len(WORKER_URLS)}"
    )

    print(
        f"Threads: "
        f"{MAX_THREADS}"
    )

    print(
        f"Starting global interval: "
        f"{START_INTERVAL}s"
    )

    print(
        f"Database: "
        f"{DATABASE_FILE}"
    )

    print(
        "========================================"
    )


    conn = connect_database()


    # Restore interval from previous run
    stored_interval = get_metadata(
        conn,
        "current_interval",
        None
    )


    if stored_interval is not None:

        pacer.set_interval(
            float(
                stored_interval
            )
        )


        print(
            f"Restored interval: "
            f"{pacer.get_interval():.2f}s"
        )


    try:

        first_pass_complete = (
            get_metadata(
                conn,
                "first_pass_complete",
                "0"
            )
        )


        if (
            first_pass_complete
            != "1"
        ):

            first_pass(
                conn,
                product_ids,
                start_time
            )

        else:

            print(
                "First pass already complete."
            )


        for round_number in range(
            1,
            RETRY_ROUNDS + 1
        ):

            retry_round(
                conn,
                round_number,
                total_ids,
                start_time
            )


        print()
        print(
            "Exporting JSON..."
        )


        success_count = (
            export_products(
                conn
            )
        )


        (
            permanent_count,
            temporary_count
        ) = export_errors(
            conn
        )


        total_classified = (

            success_count
            + permanent_count
            + temporary_count
        )


        elapsed = (
            time.time()
            - start_time
        )


        print()
        print(
            "========================================"
        )

        print(
            "FINAL RESULT"
        )

        print(
            "========================================"
        )

        print(
            f"Input IDs: "
            f"{total_ids}"
        )

        print(
            f"Successful products: "
            f"{success_count}"
        )

        print(
            f"Permanent errors: "
            f"{permanent_count}"
        )

        print(
            f"Temporary unresolved: "
            f"{temporary_count}"
        )

        print(
            f"Classified: "
            f"{total_classified}/"
            f"{total_ids}"
        )

        print(
            f"Final global interval: "
            f"{pacer.get_interval():.2f}s"
        )

        print(
            f"Session runtime: "
            f"{format_time(elapsed)}"
        )


        if (
            total_classified
            == total_ids
        ):

            print(
                "Validation: OK"
            )

        else:

            print(
                "Validation: COUNT MISMATCH"
            )


        print(
            "========================================"
        )


    except KeyboardInterrupt:

        print()
        print(
            "Crawler stopped."
        )

        print(
            "SQLite state has been saved."
        )

        print(
            "Run the same script again "
            "to resume."
        )


    finally:

        conn.commit()

        conn.close()


if __name__ == "__main__":

    main()