def _resolve_sort_by(sort_by: str) -> str:
    """Return the canonical TradingView field name for the given sort_by alias."""
    if not isinstance(sort_by, str):
        return sort_by
    key = sort_by.strip().lower().replace(" ", "_").replace("-", "_")
    alias_map: Dict[str, str] = {
        # Symbol / ticker
        "symbol": "name", "ticker": "name", "name": "name",
        # Price / close
        "price": "close", "last": "close", "close": "close",
        # Change percentage (daily)
        "change": "change", "change_%": "change", "change_percent": "change",
        "change_pct": "change", "percent_change": "change",
        # Volume
        "volume": "volume",
        # Relative volume (10 day average)
        "rel_volume": "relative_volume_10d_calc",
        "relative_volume": "relative_volume_10d_calc",
        "relative_volume_10d_calc": "relative_volume_10d_calc",
        # Market cap
        "market_cap": "market_cap_basic",
        "market_cap_basic": "market_cap_basic",
        # Price to earnings ratio
        "pe": "price_earnings_ttm", "p/e": "price_earnings_ttm",
        "p_e": "price_earnings_ttm", "price_earnings": "price_earnings_ttm",
        "price_earnings_ttm": "price_earnings_ttm",
        # EPS (diluted) TTM
        "eps_dil": "earnings_per_share_diluted_ttm",
        "eps_dil_ttm": "earnings_per_share_diluted_ttm",
        "eps_diluted": "earnings_per_share_diluted_ttm",
        "eps_diluted_ttm": "earnings_per_share_diluted_ttm",
        # EPS YoY growth TTM
        "eps_dil_growth": "earnings_per_share_diluted_yoy_growth_ttm",
        "eps_dil_growth_ttm": "earnings_per_share_diluted_yoy_growth_ttm",
        "eps_dil_growth_yoy": "earnings_per_share_diluted_yoy_growth_ttm",
        "eps_dil_growth_ttm_yoy": "earnings_per_share_diluted_yoy_growth_ttm",
        # Dividend yield current (TTM)
        "div_yield": "dividends_yield_current",
        "dividend_yield": "dividends_yield_current",
        "dividends_yield_current": "dividends_yield_current",
        "div_yield_%": "dividends_yield_current",
        "div_yield_pct": "dividends_yield_current",
        # Sector
        "sector": "sector.tr", "sector_tr": "sector.tr",
        # Analyst rating
        "analyst_rating": "AnalystRating",
        "analyst": "AnalystRating", "analystrating": "AnalystRating",
    }
    return alias_map.get(key, sort_by)


# from __future__ import annotations

import time
import threading
import requests
from typing import Optional, List, Dict, Any, Tuple, Callable
from tenacity import retry, retry_if_exception, wait_exponential, stop_after_attempt

# --------------------------------------------
# Shared token-bucket + tenacity retry factory
# --------------------------------------------

_BUCKETS_LOCK = threading.Lock()
_BUCKETS: Dict[str, "_TokenBucket"] = {}

class _TokenBucket:
    def __init__(self, capacity: float, refill_rate_per_sec: float):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.refill_rate = float(refill_rate_per_sec)  # tokens per second
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire_wait(self, need: float = 1.0) -> float:
        """Return seconds to wait until `need` tokens are available (0 if now)."""
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            if self.tokens >= need:
                self.tokens -= need
                return 0.0
            deficit = need - self.tokens
            return max(0.0, deficit / self.refill_rate)

def _get_bucket(name: str, capacity: float, rate_per_sec: float) -> _TokenBucket:
    with _BUCKETS_LOCK:
        b = _BUCKETS.get(name)
        if b is None:
            b = _TokenBucket(capacity=capacity, refill_rate_per_sec=rate_per_sec)
            _BUCKETS[name] = b
        return b

class RateLimitError(Exception):
    """Raised by callers when a local rate-limit condition is detected."""
    pass

def make_shared_rate_limiter(
    *,
    name: str = "tradingview",
    max_calls: int = 2,
    per_seconds: float = 1.0,
    burst: Optional[int] = None,
    max_attempts: int = 6,
    min_wait: float = 1.0,
    max_wait: float = 30.0,
    respect_retry_after: bool = True,
) -> Callable:
    """
    Create a decorator that enforces a process-wide shared token-bucket
    (by `name`) and retries with exponential backoff on HTTP 429 or RateLimitError.
    """
    capacity = float(burst if burst is not None else max_calls)
    rate_per_sec = float(max_calls) / float(per_seconds)

    def is_rate_limited(exc: Exception) -> bool:
        return isinstance(exc, RateLimitError) or (
            isinstance(exc, requests.HTTPError)
            and getattr(exc, "response", None) is not None
            and exc.response.status_code == 429
        )

    def before_sleep(retry_state) -> None:
        if not respect_retry_after:
            return
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code == 429:
            ra = exc.response.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except Exception:
                    pass  # fall back to tenacity's wait policy

    retry_policy = retry(
        retry=retry_if_exception(is_rate_limited),
        wait=wait_exponential(multiplier=min_wait, max=max_wait),
        stop=stop_after_attempt(max_attempts),
        reraise=True,
        before_sleep=before_sleep,
    )

    def decorator(func: Callable) -> Callable:
        def call_with_throttle(*args, **kwargs):
            bucket = _get_bucket(name, capacity, rate_per_sec)
            while True:
                wait_s = bucket.acquire_wait(1.0)
                if wait_s <= 0:
                    break
                time.sleep(wait_s)
            return func(*args, **kwargs)

        return retry_policy(call_with_throttle)

    return decorator

# ---------------------------------------------------------
# Your function updated to use the shared rate-limit maker
# ---------------------------------------------------------

def tradingview_scanner_bangladesh(
    *,
    country_path: str = "bangladesh",
    label_product: str = "screener-stock",
    columns: Optional[List[str]] = None,
    lang: str = "en",
    is_primary_only: bool = True,
    markets: Optional[List[str]] = None,
    sort_by: str = "market_cap_basic",
    sort_order: str = "desc",
    result_range: Tuple[int, int] = (0, 100),
    include_common_stock: bool = True,
    include_preferred_stock: bool = True,
    include_depository_receipts: bool = True,
    include_funds_excluding_etf: bool = True,
    extra_filter2_operands: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
    base_url: str = "https://scanner.tradingview.com",
    headers: Optional[Dict[str, str]] = None,
    price_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    change_percent_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    market_cap_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    pe_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    eps_growth_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    dividend_yield_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    sectors: Optional[List[str]] = None,
    analyst_ratings: Optional[List[str]] = None,
    rate_limiter: Optional[Callable] = None,   # can be a shared limiter from make_shared_rate_limiter
) -> Dict[str, Any]:
    """Query TradingView's Bangladesh screener with configurable filters, sorting, and shared rate-limit handling."""
    # Resolve canonical sort key
    canonical_sort_by = _resolve_sort_by(sort_by)

    if columns is None:
        columns = [
            "name", "description", "logoid", "update_mode", "type", "typespecs",
            "close", "pricescale", "minmov", "fractional", "minmove2",
            "currency", "change", "volume", "relative_volume_10d_calc",
            "market_cap_basic", "fundamental_currency_code",
            "price_earnings_ttm", "earnings_per_share_diluted_ttm",
            "earnings_per_share_diluted_yoy_growth_ttm",
            "dividends_yield_current", "sector.tr", "market", "sector",
            "AnalystRating", "AnalystRating.tr", "exchange",
        ]
    else:
        columns = list(columns)

    if canonical_sort_by not in columns:
        columns.append(canonical_sort_by)
    if canonical_sort_by.endswith(".tr"):
        base_field = canonical_sort_by[:-3]
        if base_field and base_field not in columns:
            columns.append(base_field)

    if markets is None:
        markets = ["bangladesh"]

    # Instrument type filtering
    type_operands: List[Dict[str, Any]] = []
    if include_common_stock:
        type_operands.append({
            "operation": {"operator": "and", "operands": [
                {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                {"expression": {"left": "typespecs", "operation": "has", "right": ["common"]}},
            ]}})
    if include_preferred_stock:
        type_operands.append({
            "operation": {"operator": "and", "operands": [
                {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                {"expression": {"left": "typespecs", "operation": "has", "right": ["preferred"]}},
            ]}})
    if include_depository_receipts:
        type_operands.append({
            "operation": {"operator": "and", "operands": [
                {"expression": {"left": "type", "operation": "equal", "right": "dr"}},
            ]}})
    if include_funds_excluding_etf:
        type_operands.append({
            "operation": {"operator": "and", "operands": [
                {"expression": {"left": "type", "operation": "equal", "right": "fund"}},
                {"expression": {"left": "typespecs", "operation": "has_none_of", "right": ["etf"]}},
            ]}})

    filter2_operands: List[Dict[str, Any]] = []
    if type_operands:
        filter2_operands.append({"operation": {"operator": "or", "operands": type_operands}})
    if extra_filter2_operands:
        filter2_operands.extend(extra_filter2_operands)

    # Numeric range filters
    def _add_range_filters(alias: str, rng: Optional[Tuple[Optional[float], Optional[float]]]) -> None:
        if rng is None:
            return
        low, high = rng
        field = _resolve_sort_by(alias)
        if low is not None:
            filter2_operands.append({"expression": {"left": field, "operation": "greater", "right": float(low)}})
        if high is not None:
            filter2_operands.append({"expression": {"left": field, "operation": "less", "right": float(high)}})

    _add_range_filters("price", price_range)
    _add_range_filters("change", change_percent_range)
    _add_range_filters("market_cap", market_cap_range)
    _add_range_filters("pe", pe_range)
    _add_range_filters("eps_dil_growth", eps_growth_range)
    _add_range_filters("div_yield", dividend_yield_range)

    # Sector & analyst rating
    if sectors:
        if len(sectors) == 1:
            filter2_operands.append({"expression": {"left": "sector", "operation": "equal", "right": sectors[0]}})
        else:
            filter2_operands.append({"operation": {"operator": "or", "operands": [
                {"expression": {"left": "sector", "operation": "equal", "right": sec}} for sec in sectors
            ]}})
    if analyst_ratings:
        if len(analyst_ratings) == 1:
            filter2_operands.append({"expression": {"left": "AnalystRating", "operation": "equal", "right": analyst_ratings[0]}})
        else:
            filter2_operands.append({"operation": {"operator": "or", "operands": [
                {"expression": {"left": "AnalystRating", "operation": "equal", "right": r}} for r in analyst_ratings
            ]}})

    payload: Dict[str, Any] = {
        "columns": columns,
        "filter": ([{"left": "is_primary", "operation": "equal", "right": True}] if is_primary_only else []),
        "ignore_unknown_fields": False,
        "options": {"lang": lang},
        "range": list(result_range),
        "sort": {"sortBy": canonical_sort_by, "sortOrder": sort_order},
        "symbols": {},
        "markets": markets,
        "filter2": {"operator": "and", "operands": filter2_operands} if filter2_operands else None,
    }

    default_headers = {
        "Accept": "application/json",
        "Content-Type": "text/plain;charset=UTF-8",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "DNT": "1",
    }
    if headers:
        default_headers.update(headers)

    url = f"{base_url}/{country_path}/scan"
    params = {"label-product": label_product}
    sess = session or requests.Session()

    def _make_request():
        resp = sess.post(url, params=params, headers=default_headers, json=payload, timeout=timeout)
        if resp.status_code == 429:
            # Raise HTTPError so Retry-After can be honored by the limiter
            raise requests.HTTPError("rate limited", response=resp)
        resp.raise_for_status()
        return resp.json()

    # If no limiter provided, create a shared one with sane defaults
    if rate_limiter is None:
        rate_limiter = make_shared_rate_limiter(
            name="tradingview",   # all calls share this bucket
            max_calls=2,          # 2 req/sec
            per_seconds=1.0,
            burst=2,
            max_attempts=6,
            min_wait=1.0,
            max_wait=30.0,
            respect_retry_after=True,
        )

    # Apply (or use the provided) shared limiter
    return rate_limiter(_make_request)()


"""
Test suite for the ``tradingview_scan_bangladesh`` helper.

Run: import this module and call run_all_tests().

These tests hit TradingView's live screener API via your helper, so an internet
connection is required. Any failed assertion will raise AssertionError.
"""

# from __future__ import annotations

import math
from typing import Any, List

# IMPORTANT: ensure this import works in your environment
# If your helper is in a different module/path, adjust the import accordingly.
# from tradingview_scan_bangladesh import (
#     tradingview_scan_bangladesh,
#     _resolve_sort_by,
#     make_shared_rate_limiter,  # <-- shared limiter factory
# )

def _fmt_list_preview(values: List[Any], n: int = 5) -> str:
    preview = values[:n]
    return f"{preview} (showing {min(n, len(values))}/{len(values)})"

def _info(title: str, **data: Any) -> None:
    print(f"\n[INFO] {title}")
    for k, v in data.items():
        print(f"       - {k}: {v}")

def _expect(msg: str) -> None:
    print(f"[EXPECT] {msg}")

def _pass(msg: str) -> None:
    print(f"[PASS] {msg}")

def _fail(expected: str, got: str) -> None:
    raise AssertionError(f"{expected} | Got: {got}")

def _assert_true(cond: bool, expected: str, got: str) -> None:
    if cond:
        _pass(f"{expected} | Got: {got}")
    else:
        _fail(expected, got)

def _is_sorted(values: List[Any], reverse: bool = False) -> bool:
    """Return True if the list is sorted according to ``reverse`` flag."""
    def to_key(x: Any) -> float:
        if x is None:
            return -math.inf if not reverse else math.inf
        if isinstance(x, float) and math.isnan(x):
            return -math.inf if not reverse else math.inf
        return x

    for i in range(len(values) - 1):
        a = to_key(values[i])
        b = to_key(values[i + 1])
        if reverse:
            if a < b:
                return False
        else:
            if a > b:
                return False
    return True

def run_all_tests() -> None:
    """Execute a series of assertions validating the screener helper."""

    # --- NEW: create a shared, process-wide limiter used in all calls ---
    rate_limiter = make_shared_rate_limiter(
        name="tradingview-tests",  # callers sharing this name share the bucket
        max_calls=2,               # 2 req/sec shared across tests
        per_seconds=1.0,
        burst=2,
        max_attempts=6,            # retry attempts on 429
        min_wait=1.0,              # exponential backoff base
        max_wait=30.0,
        respect_retry_after=True,
    )
    common = {"rate_limiter": rate_limiter}

    def check_sort(alias: str, order: str = "desc", n: int = 20) -> None:
        _info("Check sort", alias=alias, order=order, top_n=n)
        canonical = _resolve_sort_by(alias)
        _expect(f"Column alias '{alias}' resolves to canonical '{canonical}'")
        _pass(f"Resolved: {alias} -> {canonical}")

        resp = tradingview_scanner_bangladesh(
            columns=[canonical], sort_by=alias, sort_order=order, result_range=(0, n),
            **common # type: ignore
        )
        _expect("Response contains 'data' list")
        _assert_true("data" in resp and isinstance(resp["data"], list),
                     "resp['data'] exists and is list",
                     f"type: {type(resp.get('data'))}, length: {len(resp.get('data', []))}")

        idx = 0
        values = [row["d"][idx] for row in resp["data"]]
        reverse = order.lower() == "desc"
        _expect(f"Values are sorted in {'descending' if reverse else 'ascending'} order")
        _assert_true(_is_sorted(values, reverse=reverse),
                     "sorted correctly",
                     _fmt_list_preview(values))

    # Test default sorting by market cap descending
    _info("Test 1: Default sorting by market cap (desc)")
    resp = tradingview_scanner_bangladesh(columns=["market_cap_basic"], result_range=(0, 50), **common) # type: ignore
    _expect("Response contains 'data' list")
    _assert_true("data" in resp and isinstance(resp["data"], list),
                 "resp['data'] exists and is list",
                 f"type: {type(resp.get('data'))}, length: {len(resp.get('data', []))}")
    vals = [row["d"][0] for row in resp["data"]]
    _expect("Market cap values are sorted descending")
    _assert_true(_is_sorted(vals, reverse=True),
                 "sorted(desc) by market_cap_basic",
                 _fmt_list_preview(vals))

    # Sorting by price ascending/descending
    _info("Test 2: Sorting by price (desc/asc)")
    check_sort("price", order="desc", n=30)
    check_sort("price", order="asc", n=30)

    # Sorting by symbol ascending
    _info("Test 3: Sorting by symbol (asc)")
    check_sort("symbol", order="asc", n=50)

    # Sorting by change_percent descending
    _info("Test 4: Sorting by change_percent (desc)")
    check_sort("change_percent", order="desc", n=30)

    # Sorting by volume descending
    _info("Test 5: Sorting by volume (desc)")
    check_sort("volume", order="desc", n=30)

    # Sorting by relative volume descending
    _info("Test 6: Sorting by rel_volume (desc)")
    check_sort("rel_volume", order="desc", n=30)

    # Sorting by PE ratio descending
    _info("Test 7: Sorting by PE ratio (desc)")
    check_sort("pe", order="desc", n=30)

    # Sorting by EPS diluted TTM descending
    _info("Test 8: Sorting by EPS diluted TTM (desc)")
    check_sort("eps_dil", order="desc", n=30)

    # Sorting by EPS growth ascending
    _info("Test 9: Sorting by EPS growth (asc)")
    check_sort("eps_dil_growth", order="asc", n=30)

    # Sorting by dividend yield descending
    _info("Test 10: Sorting by dividend yield (desc)")
    check_sort("div_yield", order="desc", n=30)

    # Sorting by sector ascending
    _info("Test 11: Sorting by sector (asc)")
    check_sort("sector", order="asc", n=30)

    # Sorting by analyst rating ascending; verify types
    _info("Test 12: Sorting by analyst rating (asc) & types")
    resp = tradingview_scanner_bangladesh(
        columns=["AnalystRating"],
        sort_by="analyst_rating",
        sort_order="asc",
        result_range=(0, 10),
        **common # type: ignore
    )
    _expect("Non-empty data for analyst rating call")
    _assert_true(bool(resp["data"]), "data length > 0", f"len={len(resp['data'])}")
    for i, row in enumerate(resp["data"][:10]):
        val = row["d"][0]
        _expect(f"Row {i}: Analyst rating is string or None")
        _assert_true(isinstance(val, str) or val is None,
                     "string or None",
                     f"value={val!r}, type={type(val).__name__}")

    # Ascending vs descending consistency: price
    _info("Test 13: Ascending vs descending consistency (price)")
    resp_desc = tradingview_scanner_bangladesh(
        columns=["close"], sort_by="price", sort_order="desc", result_range=(0, 20),
        **common # type: ignore
    )
    resp_asc = tradingview_scanner_bangladesh(
        columns=["close"], sort_by="price", sort_order="asc", result_range=(0, 20),
        **common # type: ignore
    )
    desc_vals = [row["d"][0] for row in resp_desc["data"]]
    asc_vals = [row["d"][0] for row in resp_asc["data"]]
    _expect("max(asc sample) <= min(desc sample)")
    _assert_true(max(asc_vals) <= min(desc_vals),
                 "max(asc) <= min(desc)",
                 f"max(asc)={max(asc_vals)}, min(desc)={min(desc_vals)}")

    # Auto-include sort field in columns
    _info("Test 14: Auto-include sort field when not requested (sort by price)")
    resp = tradingview_scanner_bangladesh(
        columns=["name"], sort_by="price", sort_order="desc", result_range=(0, 5),
        **common # type: ignore
    )
    for i, row in enumerate(resp["data"]):
        _expect(f"Row {i}: 2 columns [name, close]")
        _assert_true(len(row["d"]) == 2,
                     "len(row['d']) == 2",
                     f"len={len(row['d'])}, row={row['d']}")
        price_val = row["d"][1]
        _expect(f"Row {i}: price is numeric or None")
        _assert_true(isinstance(price_val, (int, float)) or price_val is None,
                     "price numeric or None",
                     f"price={price_val!r}, type={type(price_val).__name__}")

    # Instrument type filtering: only depository receipts
    _info("Test 15: Only depository receipts (type == 'dr')")
    resp = tradingview_scanner_bangladesh(
        columns=["type"],
        include_common_stock=False,
        include_preferred_stock=False,
        include_depository_receipts=True,
        include_funds_excluding_etf=False,
        sort_by="market_cap_basic",
        sort_order="desc",
        result_range=(0, 50),
        **common # type: ignore
    )
    types = [row["d"][0] for row in resp["data"]]
    _expect("All returned types are 'dr'")
    _assert_true(all(t == "dr" for t in types),
                 "all(type == 'dr')",
                 _fmt_list_preview(types))

    # Numeric range filters (strict comparisons)
    _info("Test 16: Numeric range filters")
    resp = tradingview_scanner_bangladesh(columns=["close"], price_range=(100, 200), result_range=(0, 20), **common) # type: ignore
    price_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All price values strictly between 100 and 200")
    _assert_true(all(100 < v < 200 for v in price_vals),
                 "100 < price < 200",
                 _fmt_list_preview(price_vals))

    resp = tradingview_scanner_bangladesh(columns=["change"], change_percent_range=(-5, 5), result_range=(0, 20), **common) # type: ignore
    change_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All change% values strictly between -5 and 5")
    _assert_true(all(-5 < v < 5 for v in change_vals),
                 "-5 < change < 5",
                 _fmt_list_preview(change_vals))

    resp = tradingview_scanner_bangladesh(columns=["market_cap_basic"], market_cap_range=(1e10, 1e12), result_range=(0, 20), **common) # type: ignore
    cap_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All market cap values strictly between 1e10 and 1e12")
    _assert_true(all(1e10 < v < 1e12 for v in cap_vals),
                 "1e10 < market_cap < 1e12",
                 _fmt_list_preview(cap_vals))

    resp = tradingview_scanner_bangladesh(columns=["price_earnings_ttm"], pe_range=(0, 20), result_range=(0, 20), **common) # type: ignore
    pe_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All P/E values strictly between 0 and 20")
    _assert_true(all(0 < v < 20 for v in pe_vals),
                 "0 < PE < 20",
                 _fmt_list_preview(pe_vals))

    resp = tradingview_scanner_bangladesh(columns=["earnings_per_share_diluted_yoy_growth_ttm"], eps_growth_range=(-100, 100), result_range=(0, 20), **common) # type: ignore
    eps_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All EPS growth values strictly between -100 and 100")
    _assert_true(all(-100 < v < 100 for v in eps_vals),
                 "-100 < eps_growth < 100",
                 _fmt_list_preview(eps_vals))

    resp = tradingview_scanner_bangladesh(columns=["dividends_yield_current"], dividend_yield_range=(0, 10), result_range=(0, 20), **common) # type: ignore
    div_vals = [row["d"][0] for row in resp["data"] if row["d"][0] is not None]
    _expect("All dividend yield values strictly between 0 and 10")
    _assert_true(all(0 < v < 10 for v in div_vals),
                 "0 < dividend_yield < 10",
                 _fmt_list_preview(div_vals))

    # Sector inclusion filter (multi-value)
    _info("Test 17: Sector inclusion filter")
    resp = tradingview_scanner_bangladesh(columns=["sector"], result_range=(0, 50), **common) # type: ignore
    sectors_observed = []
    for row in resp["data"]:
        sec = row["d"][0]
        if sec not in sectors_observed:
            sectors_observed.append(sec)
        if len(sectors_observed) >= 2:
            break
    if not sectors_observed:
        sectors_observed = ["Finance"]
    resp = tradingview_scanner_bangladesh(columns=["sector"], sectors=sectors_observed, result_range=(0, 20), **common) # type: ignore
    sector_vals = [row["d"][0] for row in resp["data"]]
    _expect(f"All returned sectors in {sectors_observed}")
    _assert_true(all(v in sectors_observed for v in sector_vals),
                 "sector filter applied",
                 _fmt_list_preview(sector_vals))

    # Analyst rating inclusion filter (multi-value)
    _info("Test 18: Analyst rating inclusion filter")
    resp = tradingview_scanner_bangladesh(columns=["AnalystRating"], result_range=(0, 50), **common) # type: ignore
    ratings_observed = []
    for row in resp["data"]:
        val = row["d"][0]
        if val and val not in ratings_observed:
            ratings_observed.append(val)
        if len(ratings_observed) >= 2:
            break
    if not ratings_observed:
        ratings_observed = ["StrongBuy"]
    resp = tradingview_scanner_bangladesh(columns=["AnalystRating"], analyst_ratings=ratings_observed, result_range=(0, 20), **common) # type: ignore
    rating_vals = [row["d"][0] for row in resp["data"]]
    _expect(f"All returned analyst ratings in {ratings_observed}")
    _assert_true(all(v in ratings_observed for v in rating_vals),
                 "analyst rating filter applied",
                 _fmt_list_preview(rating_vals))

    # Range limiting: number of rows == end - start
    _info("Test 19: Range limiting")
    start, end = 10, 25
    resp = tradingview_scanner_bangladesh(columns=["market_cap_basic"], result_range=(start, end), **common) # type: ignore
    expected_len = max(0, end - start)
    got_len = len(resp["data"])
    _expect(f"Returned rows == {expected_len} for range ({start}, {end})")
    _assert_true(got_len == expected_len,
                 "len(data) == expected_length",
                 f"len(data)={got_len}, expected={expected_len}")

    print("\n✅ All tests passed successfully.")



# from datetime import datetime
# import os
# import pandas as pd

# def save_dse_screener(df: pd.DataFrame, save_as_csv: bool = False) -> str:
#     """
#     Save DataFrame as CSV in Kaggle with filename pattern dse-screener-MM-DD-YYYY.csv.
    
#     Args:
#         df (pd.DataFrame): The DataFrame to save.
#         save_as_csv (bool): If True, saves the CSV; if False, does nothing.
    
#     Returns:
#         str: The file path if saved, or an empty string if not saved.
#     """
#     if not save_as_csv:
#         print("ℹ️ save_as_csv=False — skipping save.")
#         return ""

#     # Directory for saving
#     # save_dir = "/data/"
#     # os.makedirs(save_dir, exist_ok=True)

#         # Resolve folder relative to repo root
#     repo_root = os.path.dirname(os.path.dirname(__file__))  # one level up from scripts/
#     save_dir = os.path.join(repo_root, "data")
#     os.makedirs(save_dir, exist_ok=True)  # make sure folder exists

#     # Create filename in format: dse-screener-MM-DD-YYYY.csv
#     filename = f"dse-screener-{datetime.now().strftime('%m-%d-%Y')}.csv"
#     filepath = os.path.join(save_dir, filename)

#     # Save without overwriting
#     if not os.path.exists(filepath):
#         df.to_csv(filepath, index=False)
#         print(f"✅ Saved: {filepath}")
#         return filepath
#     else:
#         print(f"⚠️ File already exists, not overwriting: {filepath}")
#         return filepath


import os
from datetime import datetime
import pandas as pd

def save_dse_screener(df: pd.DataFrame, save_as_csv: bool = False, _object: str = "screener") -> str:
    """
    Save DataFrame as CSV in repo with filename pattern dse-<object>-MM-DD-YYYY.csv.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        save_as_csv (bool): If True, saves the CSV; if False, does nothing.
        _object (str): Name of subfolder (e.g., 'screener', 'abc').

    Returns:
        str: The file path if saved, or an empty string if not saved.
    """
    if not save_as_csv:
        print("ℹ️ save_as_csv=False — skipping save.")
        return ""

    # Resolve folder relative to repo root
    repo_root = os.path.dirname(os.path.dirname(__file__))  # one level up from scripts/
    save_dir = os.path.join(repo_root, "data", _object)
    os.makedirs(save_dir, exist_ok=True)

    # Create filename in format: dse-<object>-MM-DD-YYYY.csv
    filename = f"dse-{_object}-{datetime.now().strftime('%m-%d-%Y')}.csv"
    filepath = os.path.join(save_dir, filename)

    # Save without overwriting
    if not os.path.exists(filepath):
        df.to_csv(filepath, index=False)
        print(f"✅ Saved: {filepath}")
    else:
        print(f"⚠️ File already exists, not overwriting: {filepath}")

    return filepath



