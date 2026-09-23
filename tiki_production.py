import csv
import itertools
import json
import os
import re
import sqlite3
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup


##########
# CONFIG
##########

from config import (
    CHUNK_SIZE,
    CLEAN_CHUNKS_TO_SPEED_UP,
    DATABASE_FILE,
    ERROR_DIR,
    FALLBACK_ON,
    HIGH_CHALLENGE_RATE,
    HIGH_COOLDOWN,
    INPUT_FILE,
    LOW_CHALLENGE_RATE,
    LOW_COOLDOWN,
    MAX_INTERVAL,
    MAX_THREADS,
    MEDIUM_CHALLENGE_RATE,
    MEDIUM_COOLDOWN,
    MIN_INTERVAL,
    OUTPUT_DIR,
    PERMANENT_ERROR_FILE,
    PRODUCTS_PER_FILE,
    REQUEST_TIMEOUT,
    RETRY_MIN_INTERVAL,
    RETRY_ROUNDS,
    RETRY_WAIT,
    SLOW_DOWN_HIGH,
    SLOW_DOWN_MEDIUM,
    SLOW_DOWN_SMALL,
    SPEED_UP_STEP,
    START_INTERVAL,
    STATE_DIR,
    TEMPORARY_ERROR_FILE,
    TEST_LIMIT,
    WORKER_URLS,
)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)


##########
# HELPERS
##########

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

        reader = csv.DictReader(
            file
        )

        for row in reader:

            product_id = row.get(
                "id"
            )

            if product_id:

                product_ids.append(
                    str(
                        product_id
                    ).strip()
                )

    # Remove duplicate IDs,
    # preserve original order.
    return list(
        dict.fromkeys(
            product_ids
        )
    )


##########
# THREAD-LOCAL REQUEST SESSION
##########

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


##########
# PRECISE SHARED RATE LIMITER
##########

class SharedRateLimiter:
    """
    Each caller reserves an absolute future request slot.

    Important difference from the old pacer:

        - lock is held only while assigning a slot
        - sleep happens OUTSIDE the lock
        - threads can reserve future slots independently

    Example interval = 1.05:

        Thread A -> slot 0.00
        Thread B -> slot 1.05
        Thread C -> slot 2.10
        Thread D -> slot 3.15
        Thread E -> slot 4.20

    This does NOT intentionally increase the configured
    aggregate request-start rate.
    """

    def __init__(
        self,
        interval
    ):

        self.interval = interval

        self.lock = threading.Lock()

        self.next_slot = 0.0


    def wait(self):

        with self.lock:

            now = time.monotonic()

            slot = max(
                now,
                self.next_slot
            )

            self.next_slot = (
                slot
                + self.interval
            )


        # Sleep outside lock.
        while True:

            remaining = (
                slot
                - time.monotonic()
            )

            if remaining <= 0:
                break

            time.sleep(
                remaining
            )


    def set_interval(
        self,
        value
    ):

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


rate_limiter = SharedRateLimiter(
    START_INTERVAL
)


##########
# METRICS
##########

metrics_lock = threading.Lock()

total_http_requests = 0
total_fallbacks = 0

total_request_latency = 0.0

total_processing_time = 0.0
total_processing_count = 0

request_start_times = []


def record_request_start():

    global total_http_requests

    timestamp = time.perf_counter()

    with metrics_lock:

        total_http_requests += 1

        request_start_times.append(
            timestamp
        )

    return timestamp


def record_request_latency(
    latency
):

    global total_request_latency

    with metrics_lock:

        total_request_latency += latency


def record_processing_time(
    processing_time
):

    global total_processing_time
    global total_processing_count

    with metrics_lock:

        total_processing_time += (
            processing_time
        )

        total_processing_count += 1


def record_fallback():

    global total_fallbacks

    with metrics_lock:

        total_fallbacks += 1


##########
# ENDPOINT POOL
##########

endpoint_lock = threading.Lock()

endpoint_counter = itertools.count()


def get_next_endpoint(
    exclude=None
):

    if exclude is None:

        exclude = set()


    with endpoint_lock:

        for _ in range(
            len(
                WORKER_URLS
            )
        ):

            index = (
                next(
                    endpoint_counter
                )
                % len(
                    WORKER_URLS
                )
            )

            endpoint = (
                WORKER_URLS[
                    index
                ]
            )

            if endpoint not in exclude:

                return endpoint

    return None


##########
# DATABASE
##########

def connect_database():

    conn = sqlite3.connect(
        DATABASE_FILE
    )


    # Better for long-running ingestion workloads.
    conn.execute(
        "PRAGMA journal_mode=WAL"
    )

    conn.execute(
        "PRAGMA synchronous=NORMAL"
    )

    conn.execute(
        "PRAGMA temp_store=MEMORY"
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


    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_results_status
        ON results(status)
        """
    )


    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_results_status_attempts
        ON results(status, attempts)
        """
    )


    conn.commit()

    return conn


def set_metadata(
    conn,
    key,
    value,
    commit=False
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


    if commit:

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


##########
# RUNTIME
##########

def checkpoint_runtime(
    conn,
    session_start_time,
    previous_runtime
):

    current_session_runtime = (
        time.time()
        - session_start_time
    )

    total_runtime = (
        previous_runtime
        + current_session_runtime
    )


    set_metadata(
        conn,
        "total_runtime_seconds",
        total_runtime,
        commit=False
    )


    set_metadata(
        conn,
        "last_checkpoint_at",
        datetime.now().isoformat(
            timespec="seconds"
        ),
        commit=False
    )


    conn.commit()


    return total_runtime


def get_total_runtime(
    session_start_time,
    previous_runtime
):

    return (
        previous_runtime
        +
        (
            time.time()
            - session_start_time
        )
    )


##########
# SAVE RESULT
##########

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


##########
# REQUEST PRODUCT
##########

def request_product(
    product_id,
    endpoint,
    attempt
):

    # Reserve aggregate request slot.
    rate_limiter.wait()


    session = get_session()


    url = (
        f"{endpoint}/"
        f"{product_id}"
    )


    request_start = (
        record_request_start()
    )


    response_received_time = None


    try:

        response = session.get(
            url,
            timeout=REQUEST_TIMEOUT
        )


        response_received_time = (
            time.perf_counter()
        )


        record_request_latency(
            response_received_time
            - request_start
        )


        
        # 404
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


       
        # HTTP ERROR
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


       
        # EMPTY
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


        
        # HTML CHALLENGE
        content_type = (
            response.headers
            .get(
                "Content-Type",
                ""
            )
            .lower()
        )


        stripped_text = (
            text
            .lstrip()
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


        
        # JSON
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


        if not isinstance(
            data,
            dict
        ):

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    (
                        "Unexpected JSON type: "
                        f"{type(data).__name__}"
                    ),

                "error_type":
                    "invalid_json_structure",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        
        # VERIFY PRODUCT ID
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
            str(
                returned_id
            )
            !=
            str(
                product_id
            )
        ):

            return {

                "product_id":
                    product_id,

                "status":
                    "temporary_error",

                "reason":
                    (
                        "ID mismatch: "
                        f"{returned_id}"
                    ),

                "error_type":
                    "id_mismatch",

                "attempts":
                    attempt,

                "endpoint":
                    endpoint
            }


        
        # IMAGES
        images_url = []


        # Handle null images
        images = (
            data.get(
                "images"
            )
            or []
        )


        if not isinstance(
            images,
            list
        ):

            images = []


        for image in images:

            if not isinstance(
                image,
                dict
            ):

                continue


            image_url = image.get(
                "base_url"
            )


            if image_url:

                images_url.append(
                    image_url
                )


        
        # PRODUCT
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


    
    # TIMEOUT
    except requests.exceptions.Timeout:

        record_request_latency(
            time.perf_counter()
            - request_start
        )


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


    ##########
    # CONNECTION
    ##########
    
    except requests.exceptions.ConnectionError as error:

        record_request_latency(
            time.perf_counter()
            - request_start
        )


        return {

            "product_id":
                product_id,

            "status":
                "temporary_error",

            "reason":
                (
                    "ConnectionError: "
                    f"{error}"
                ),

            "error_type":
                "connection",

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


    
    # REQUEST ERROR
    except requests.exceptions.RequestException as error:

        record_request_latency(
            time.perf_counter()
            - request_start
        )


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


    
    # PARSER / OTHER ERROR
    except Exception as error:

        return {

            "product_id":
                product_id,

            "status":
                "temporary_error",

            "reason":
                (
                    "Unexpected parser error: "
                    f"{type(error).__name__}: "
                    f"{error}"
                ),

            "error_type":
                "parser_error",

            "attempts":
                attempt,

            "endpoint":
                endpoint
        }


    finally:

        if (
            response_received_time
            is not None
        ):

            record_processing_time(

                time.perf_counter()
                - response_received_time
            )


##########
# FETCH PRODUCT + FALLBACK
##########

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


    if (
        first_result[
            "status"
        ]
        != "temporary_error"
    ):

        return first_result


    error_type = first_result.get(
        "error_type"
    )


    # Challenge is deferred.
    if error_type == "challenge":

        return first_result


    # Only endpoint/network failures
    # get immediate fallback.
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


    record_fallback()


    fallback_result = request_product(
        product_id,
        fallback_endpoint,
        attempt
    )


    if (
        fallback_result[
            "status"
        ]
        == "success"
    ):

        fallback_result[
            "reason"
        ] = None


    return fallback_result


##########
# RUN ONE CONTROL CHUNK
##########

def run_chunk(
    conn,
    executor,
    product_ids,
    attempt
):

    chunk_start_time = (
        time.perf_counter()
    )


    # Snapshot metrics.
    with metrics_lock:

        start_http_requests = (
            total_http_requests
        )

        start_fallbacks = (
            total_fallbacks
        )

        start_latency = (
            total_request_latency
        )

        start_processing_time = (
            total_processing_time
        )

        start_processing_count = (
            total_processing_count
        )

        start_request_index = (
            len(
                request_start_times
            )
        )


    stats = {

        "success": 0,

        "permanent": 0,

        "temporary": 0,

        "challenge": 0
    }


    # IMPORTANT:
    #
    # executor is persistent.
    # We do NOT create a new ThreadPoolExecutor per chunk.
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


    # One commit for all 100 result rows.
    conn.commit()


    chunk_runtime = (
        time.perf_counter()
        - chunk_start_time
    )


    # METRICS
    with metrics_lock:

        chunk_http_requests = (
            total_http_requests
            - start_http_requests
        )

        chunk_fallbacks = (
            total_fallbacks
            - start_fallbacks
        )

        chunk_total_latency = (
            total_request_latency
            - start_latency
        )

        chunk_processing_time = (
            total_processing_time
            - start_processing_time
        )

        chunk_processing_count = (
            total_processing_count
            - start_processing_count
        )

        chunk_start_times = (
            request_start_times[
                start_request_index:
            ].copy()
        )


    if chunk_http_requests > 0:

        avg_latency = (
            chunk_total_latency
            / chunk_http_requests
        )

    else:

        avg_latency = 0.0


    if chunk_processing_count > 0:

        avg_processing_time = (
            chunk_processing_time
            / chunk_processing_count
        )

    else:

        avg_processing_time = 0.0


    start_gaps = []


    for index in range(
        1,
        len(
            chunk_start_times
        )
    ):

        start_gaps.append(

            chunk_start_times[
                index
            ]
            -
            chunk_start_times[
                index - 1
            ]
        )


    if start_gaps:

        avg_start_gap = (
            sum(
                start_gaps
            )
            / len(
                start_gaps
            )
        )

        min_start_gap = min(
            start_gaps
        )

        max_start_gap = max(
            start_gaps
        )

    else:

        avg_start_gap = 0.0
        min_start_gap = 0.0
        max_start_gap = 0.0


    if chunk_runtime > 0:

        chunk_rate = (
            len(
                product_ids
            )
            / chunk_runtime
            * 60
        )

    else:

        chunk_rate = 0.0


    stats[
        "http_requests"
    ] = chunk_http_requests

    stats[
        "fallbacks"
    ] = chunk_fallbacks

    stats[
        "avg_latency"
    ] = avg_latency

    stats[
        "avg_processing_time"
    ] = avg_processing_time

    stats[
        "avg_start_gap"
    ] = avg_start_gap

    stats[
        "min_start_gap"
    ] = min_start_gap

    stats[
        "max_start_gap"
    ] = max_start_gap

    stats[
        "chunk_runtime"
    ] = chunk_runtime

    stats[
        "chunk_rate"
    ] = chunk_rate


    return stats


##########
# PRINT CHUNK STATS
##########

def print_chunk_stats(
    stats
):

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


    print(
        f"http_requests="
        f"{stats['http_requests']}"
        f" | fallbacks="
        f"{stats['fallbacks']}"
    )


    print(
        f"avg_latency="
        f"{stats['avg_latency']:.3f}s"
        f" | avg_processing="
        f"{stats['avg_processing_time']:.3f}s"
    )


    print(
        f"configured_interval="
        f"{rate_limiter.get_interval():.2f}s"
    )


    print(
        f"actual_start_gap="
        f"{stats['avg_start_gap']:.3f}s avg"
        f" | "
        f"{stats['min_start_gap']:.3f}s min"
        f" | "
        f"{stats['max_start_gap']:.3f}s max"
    )


    print(
        f"chunk_runtime="
        f"{format_time(stats['chunk_runtime'])}"
        f" | chunk_rate="
        f"{stats['chunk_rate']:.2f} IDs/min"
    )


##########
# ADAPTIVE PACING
##########

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
        rate_limiter.get_interval()
    )


    clean_streak = int(
        get_metadata(
            conn,
            "clean_streak",
            "0"
        )
    )


    
    # HIGH
    if (
        challenge_rate
        >= HIGH_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_HIGH
        )


        rate_limiter.set_interval(
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
            new_interval,
            commit=False
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak,
            commit=False
        )

        conn.commit()


        time.sleep(
            HIGH_COOLDOWN
        )

        return


    
    # MEDIUM
    if (
        challenge_rate
        >= MEDIUM_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_MEDIUM
        )


        rate_limiter.set_interval(
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
            new_interval,
            commit=False
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak,
            commit=False
        )

        conn.commit()


        time.sleep(
            MEDIUM_COOLDOWN
        )

        return


    
    # LOW
    if (
        challenge_rate
        >= LOW_CHALLENGE_RATE
    ):

        new_interval = min(
            MAX_INTERVAL,
            current_interval
            + SLOW_DOWN_SMALL
        )


        rate_limiter.set_interval(
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

        print(
            f"Cooldown "
            f"{LOW_COOLDOWN}s"
        )


        set_metadata(
            conn,
            "current_interval",
            new_interval,
            commit=False
        )

        set_metadata(
            conn,
            "clean_streak",
            clean_streak,
            commit=False
        )

        conn.commit()


        time.sleep(
            LOW_COOLDOWN
        )

        return


    
    # CLEAN
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

                rate_limiter.set_interval(
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
        rate_limiter.get_interval(),
        commit=False
    )


    set_metadata(
        conn,
        "clean_streak",
        clean_streak,
        commit=False
    )


    conn.commit()


##########
# COUNTS
##########

def get_counts(
    conn
):

    counts = {

        "success": 0,

        "permanent_error": 0,

        "temporary_error": 0
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


    for (
        status,
        count
    ) in rows:

        counts[
            status
        ] = count


    return counts


def get_classified_count(
    conn
):

    counts = get_counts(
        conn
    )


    return (
        counts[
            "success"
        ]
        +
        counts[
            "permanent_error"
        ]
        +
        counts[
            "temporary_error"
        ]
    )


##########
# PENDING IDS
##########

def get_pending_ids(
    conn,
    product_ids
):

    existing = {

        row[0]

        for row
        in conn.execute(
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


##########
# PROGRESS
##########

def print_progress(
    conn,
    total_ids,
    session_start_time,
    previous_runtime,
    session_start_classified,
    phase
):

    counts = get_counts(
        conn
    )


    classified = (
        counts[
            "success"
        ]
        +
        counts[
            "permanent_error"
        ]
        +
        counts[
            "temporary_error"
        ]
    )


    session_elapsed = (
        time.time()
        - session_start_time
    )


    total_runtime = (
        previous_runtime
        + session_elapsed
    )


    session_processed = max(
        0,
        classified
        - session_start_classified
    )


    if session_elapsed > 0:

        session_rate = (
            session_processed
            / session_elapsed
            * 60
        )

    else:

        session_rate = 0.0


    remaining = max(
        0,
        total_ids
        - classified
    )


    if session_rate > 0:

        eta = (
            remaining
            / session_rate
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
        f"{rate_limiter.get_interval():.2f}s"
    )

    print(
        f"Session runtime: "
        f"{format_time(session_elapsed)}"
    )

    print(
        f"Total runtime: "
        f"{format_time(total_runtime)}"
    )

    print(
        f"Session rate: "
        f"{session_rate:.2f} IDs/min"
    )

    print(
        f"ETA: "
        f"{format_time(eta)}"
    )

    print(
        "========================================"
    )


##########
# FIRST PASS
##########

def first_pass(
    conn,
    executor,
    product_ids,
    session_start_time,
    previous_runtime,
    session_start_classified
):

    pending_ids = get_pending_ids(
        conn,
        product_ids
    )


    print(
        f"First-pass pending: "
        f"{len(pending_ids)}"
    )


    if not pending_ids:

        return


    total_chunks = (
        len(
            pending_ids
        )
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

        chunk = (
            pending_ids[
                start:end
            ]
        )


        stats = run_chunk(
            conn,
            executor,
            chunk,
            attempt=1
        )


        print()

        print(
            f"Chunk "
            f"{chunk_number + 1}/"
            f"{total_chunks}"
        )


        print_chunk_stats(
            stats
        )


        adjust_pacing(
            conn,
            stats,
            len(
                chunk
            )
        )


        print_progress(
            conn,
            len(
                product_ids
            ),
            session_start_time,
            previous_runtime,
            session_start_classified,
            "FIRST PASS"
        )


        checkpoint_runtime(
            conn,
            session_start_time,
            previous_runtime
        )


##########
# RETRY
##########

def retry_round(
    conn,
    executor,
    round_number,
    total_ids,
    session_start_time,
    previous_runtime,
    session_start_classified
):

    rows = conn.execute(
        """
        SELECT product_id

        FROM results

        WHERE
            status = 'temporary_error'

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

        for row
        in rows
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
        rate_limiter.get_interval()
        < RETRY_MIN_INTERVAL
    ):

        rate_limiter.set_interval(
            RETRY_MIN_INTERVAL
        )


    total_chunks = (
        len(
            retry_ids
        )
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

        chunk = (
            retry_ids[
                start:end
            ]
        )


        stats = run_chunk(
            conn,
            executor,
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


        print_chunk_stats(
            stats
        )


        adjust_pacing(
            conn,
            stats,
            len(
                chunk
            )
        )


        print_progress(
            conn,
            total_ids,
            session_start_time,
            previous_runtime,
            session_start_classified,
            f"RETRY {round_number}"
        )


        checkpoint_runtime(
            conn,
            session_start_time,
            previous_runtime
        )


##########
# ATOMIC JSON SAVE
##########

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


##########
# EXPORT PRODUCTS
##########

def export_products(
    conn
):

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

        WHERE status = 'success'

        ORDER BY rowid
        """
    )


    batch = []

    batch_number = 1

    total = 0


    for row in rows:

        if row[0] is None:

            continue


        batch.append(
            json.loads(
                row[0]
            )
        )


        total += 1


        if (
            len(
                batch
            )
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


##########
# EXPORT ERRORS
##########

def export_errors(
    conn
):

    permanent_rows = conn.execute(
        """
        SELECT
            product_id,
            reason,
            error_type,
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
            error_type,
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

            "product_id":
                row[0],

            "reason":
                row[1],

            "error_type":
                row[2],

            "attempts":
                row[3],

            "endpoint":
                row[4]
        }

        for row
        in permanent_rows
    ]


    temporary = [

        {

            "product_id":
                row[0],

            "reason":
                row[1],

            "error_type":
                row[2],

            "attempts":
                row[3],

            "endpoint":
                row[4]
        }

        for row
        in temporary_rows
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
        len(
            permanent
        ),
        len(
            temporary
        )
    )


##########
# MAIN
##########

def main():

    session_start_time = (
        time.time()
    )


    product_ids = load_product_ids(
        INPUT_FILE
    )


    if TEST_LIMIT is not None:

        product_ids = (
            product_ids[
                :TEST_LIMIT
            ]
        )


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
        f"Minimum global interval: "
        f"{MIN_INTERVAL}s"
    )

    print(
        f"Database: "
        f"{DATABASE_FILE}"
    )

    print(
        "========================================"
    )


    conn = connect_database()


    # RUNTIME STATE
    previous_runtime = float(
        get_metadata(
            conn,
            "total_runtime_seconds",
            "0"
        )
    )


    first_started_at = (
        get_metadata(
            conn,
            "first_started_at",
            None
        )
    )


    if first_started_at is None:

        first_started_at = (
            datetime.now().isoformat(
                timespec="seconds"
            )
        )


        set_metadata(
            conn,
            "first_started_at",
            first_started_at,
            commit=False
        )


    session_count = int(
        get_metadata(
            conn,
            "session_count",
            "0"
        )
    )


    session_count += 1


    set_metadata(
        conn,
        "session_count",
        session_count,
        commit=False
    )


    set_metadata(
        conn,
        "last_started_at",
        datetime.now().isoformat(
            timespec="seconds"
        ),
        commit=False
    )


    conn.commit()


    print(
        f"Previous runtime: "
        f"{format_time(previous_runtime)}"
    )

    print(
        f"Session number: "
        f"{session_count}"
    )

    print(
        f"First started at: "
        f"{first_started_at}"
    )


    session_start_classified = (
        get_classified_count(
            conn
        )
    )


    # RESTORE ADAPTIVE INTERVAL
    stored_interval = (
        get_metadata(
            conn,
            "current_interval",
            None
        )
    )


    if stored_interval is not None:

        rate_limiter.set_interval(
            float(
                stored_interval
            )
        )


        print(
            f"Restored interval: "
            f"{rate_limiter.get_interval():.2f}s"
        )


    try:

        # ONE PERSISTENT EXECUTOR
        with ThreadPoolExecutor(
            max_workers=MAX_THREADS
        ) as executor:


            # FIRST PASS
            first_pass(
                conn,
                executor,
                product_ids,
                session_start_time,
                previous_runtime,
                session_start_classified
            )

       
            # RETRY
            for round_number in range(
                1,
                RETRY_ROUNDS + 1
            ):

                retry_round(
                    conn,
                    executor,
                    round_number,
                    total_ids,
                    session_start_time,
                    previous_runtime,
                    session_start_classified
                )


        
        # EXPORT
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


        session_runtime = (
            time.time()
            - session_start_time
        )


        total_runtime = (
            get_total_runtime(
                session_start_time,
                previous_runtime
            )
        )

        
        # FINAL
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
            f"{rate_limiter.get_interval():.2f}s"
        )

        print(
            f"Session runtime: "
            f"{format_time(session_runtime)}"
        )

        print(
            f"Total runtime: "
            f"{format_time(total_runtime)}"
        )

        print(
            f"Sessions: "
            f"{session_count}"
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
            "Run the script again "
            "to resume."
        )


    finally:

        checkpoint_runtime(
            conn,
            session_start_time,
            previous_runtime
        )

        conn.commit()

        conn.close()


##########
# ENTRY POINT
##########

if __name__ == "__main__":

    main()
