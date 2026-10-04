from collections import deque
import concurrent.futures 
from concurrent.futures import ThreadPoolExecutor, as_completed 
import gc, json, random, threading, time, psutil, requests, certifi 
from datetime import datetime, UTC 
from urllib.parse import urlparse, parse_qs
from pipeline_funcs.s3_bucket import s3_return, s3_create_dir, s3_ingest

_rate_lock = threading.Lock()
next_allowed = 0.0 

class RateLim:
    def __init__(self, rps: float = 2.0, min_rps: float = 1.0, max_rps: float = 5.0, step_up: float = 0.5, step_down: float = 1.0, eval_every: int = 50, window_size: int = 50, max_error_rate: float = 0.05,
        max_429_rate: float = 0.02
    ):
        self.rps = rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self.step_up = step_up
        self.step_down = step_down
        self.eval_every = eval_every
        self.window_size = window_size
        self.max_error_rate = max_error_rate
        self.max_429_rate = max_429_rate

        self.lock = threading.Lock()
        self.status_window = deque(maxlen = window_size)
        self.total_seen = 0

    def get_rps(self):

        with self.lock:
            return self.rps

    def set_rps(self, new_rps: float):
        
        with self.lock:
            self.rps = max(self.min_rps, min(self.max_rps, new_rps))


    def record_status(self, status_code: int | None):

        with self.lock:
            self.status_window.append(status_code)
            self.total_seen += 1

            if len(self.status_window) < self.window_size:
                return self.rps, None

            if self.total_seen % self.eval_every != 0:
                return self.rps, None

            window = list(self.status_window)

            n = len(window)
            n_429 = sum(1 for x in window if x == 429)
            n_retryable = sum(1 for x in window if x in (429, 502, 503, 504))
            n_fail = sum(1 for x in window if x is None or x >= 400)

            rate_429 = n_429 / n
            error_rate = n_fail / n
            retryable_rate = n_retryable / n

            action = None

            if rate_429 > self.max_429_rate or retryable_rate > self.max_error_rate:
                old_rps = self.rps
                self.rps = max(self.min_rps, self.rps - self.step_down)
                if self.rps != old_rps:
                    action = f"down to {self.rps:.1f}"

            elif error_rate == 0:
                old_rps = self.rps
                self.rps = min(self.max_rps, self.rps + self.step_up)
                if self.rps != old_rps:
                    action = f"up to {self.rps:.1f}"

            return self.rps, action


#throttle function for pbp & shift data scrapes 
def throttle(rate_limiter: RateLim):

    global next_allowed
    rps = rate_limiter.get_rps()
    min_interval = 1.0 / rps

    with _rate_lock:
        now = time.monotonic()

        if now < next_allowed:
            sleep = next_allowed - now
            next_allowed = next_allowed + min_interval
        else:
            sleep = 0.0
            next_allowed = now + min_interval

    if sleep > 0:
        time.sleep(sleep)
    
    if sleep >= 7: 
        print(f"Throttling request for {sleep:2f} seconds")


def driver_mem_pct() -> float:

    return psutil.virtual_memory().percent


def memory_check(api_data: list, memory_limit_pct: float) -> bool:

    return len(api_data) > 0 and driver_mem_pct() >= memory_limit_pct 


def chunk_list(items, chunk_size):
    
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def scrape_batch(urls: list[str], endpoint: str, max_workers: int = 5, starting_rps: float = 2.0, request_key: str | int | None = None, optional_headers: dict | None = None, push_to_s3: bool = False):

    rate_limiter = RateLim(rps = starting_rps)
    results = [] 
    completed_count = 0
    with ThreadPoolExecutor(max_workers = max_workers) as executor:

        future_to_url = {executor.submit(call_api, url, rate_limiter, endpoint = endpoint, request_key = None, custom_headers = optional_headers, s3_push = push_to_s3): url for url in urls}

        for future in as_completed(future_to_url):
            url = future_to_url[future]

            try:
                row = future.result()
            except Exception as e:
                print(f"Exception occured during scrape for {endpoint.lower()} request_key: {request_key}")
                if request_key is not None:
                    req_key = request_key

                elif endpoint.lower() == "pbp":
                    req_key = int(url.rsplit("/", 1)[0].rsplit("/", 1)[-1])

                elif endpoint.lower() == "shift":
                    req_key = int(url.split("gameId=")[-1])

                elif endpoint.lower() == "player_search":
                    req_key = url.split('/')[-1]

                elif endpoint.lower() == "schedule":
                    req_key = url.split('/')[-1]

                elif endpoint.lower() == "team_details":
                    req_key = None
                
                elif endpoint.lower() == "vegas_totals":
                    query_params = parse_qs(urlparse(url).query)
                    req_key = query_params.get("date", [None])[0]

                else:
                    req_key = url
                    
                row = {
                    
                    "endpoint": f"{endpoint}",
                    "request_key": req_key,
                    "http_status": None,
                    "payload": None,
                    "api_url": url,
                    "scrape_ts_utc": None
    
                }

            results.append(row)
            completed_count += 1
            if completed_count % 500 == 0:
                print(f"completed {completed_count:,} of {len(urls):,} scrapes in current batch")


    return results 


def call_api(url: str, rate_limiter: RateLim, endpoint: str, request_key: str | int | None = None, max_attempts: int = 3, time_out: int = 20, custom_headers: dict | None = None, s3_push: bool = False):
    
    if request_key is not None:
        req_key = request_key

    elif endpoint.lower() == "pbp":
        req_key = int(url.rsplit("/", 1)[0].rsplit("/", 1)[-1])

    elif endpoint.lower() == "shift":
        req_key = int(url.split("gameId=")[-1])

    elif endpoint.lower() == "player_search":
        req_key = url.split('/')[-1]
    
    elif endpoint.lower() == "schedule":
        req_key = url.split('/')[-1]
    
    elif endpoint.lower() == "team_details":
        req_key = None

    elif endpoint.lower() == "vegas_totals":
        query_params = parse_qs(urlparse(url).query)
        req_key = query_params.get("date", [None])[0]

    else:
        req_key = url

    for attempt in range(max_attempts):

        throttle(rate_limiter = rate_limiter)
        try:
            scrape_ts_utc = datetime.now(UTC).replace(tzinfo = None)
            response = requests.get(url, timeout = time_out, headers = custom_headers)
            last_status = response.status_code
            new_rps, action = rate_limiter.record_status(last_status)

            if action: 
                print(f"Adjusted RPS: {action}")

            if last_status == 200:
                payload = response.json()

                if isinstance(payload, list):
                    payload = {"data": payload}

                api_response = {

                    "endpoint": f"{endpoint}",
                    "request_key": req_key,
                    "http_status": int(last_status),
                    "payload": json.dumps(payload, ensure_ascii = False),
                    "api_url": url, 
                    "scrape_ts_utc": scrape_ts_utc,

                } 

                if s3_push:
                    print(s3_ingest(api_response, s3_ext_vol = endpoint, backfill = False))
                    
                return api_response
                    

            if last_status in (429, 502, 503, 504):
                time.sleep((2 ** attempt) + random.random())
                continue

            api_response = {
                
                "endpoint": f"{endpoint}",
                "request_key": req_key,
                "http_status": int(last_status),
                "payload": None,
                "api_url": url,
                "scrape_ts_utc": scrape_ts_utc
            } 

            if s3_push:
                print(s3_ingest(api_response, s3_ext_vol = endpoint, backfill = False))

            return api_response 
        
        except (requests.RequestException, ValueError):
            
            new_rps, action = rate_limiter.record_status(None)
            time.sleep((2 ** attempt) + random.random())

    return {

        "endpoint": f"{endpoint}",
        "request_key": req_key,
        "http_status": None,
        "payload": None,
        "api_url": url, 
        "scrape_ts_utc": None

    } 
