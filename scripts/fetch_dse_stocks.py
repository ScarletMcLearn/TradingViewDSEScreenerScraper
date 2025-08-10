# scripts/fetch_dse_market_stock.py
from __future__ import annotations

import requests
import pandas as pd
import numpy as np
import time
from typing import List, Tuple, Dict, Optional, Callable

from scripts.utils import make_shared_rate_limiter, save_dse_data

def fetch_dse_market_stock_df(
    rate_limiter: Optional[Callable] = None,
    columns: Optional[List[str]] = None,
    page_size: int = 100,
) -> pd.DataFrame:
    """
    Fetch all Dhaka Stock Exchange listings via TradingView's markets-screener API.

    Parameters
    ----------
    rate_limiter : Callable, optional
        A shared rate limiter created by make_shared_rate_limiter.  If None, a
        sensible default is used (2 calls per second, up to 6 retries on 429).
    columns : list[str], optional
        List of API fields to return.  If None, a standard set of fundamental
        columns is used.
    page_size : int
        Number of rows to request per API call (default: 100).  Must be <= 100.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing one row per DSE listing, with columns matching
        the TradingView API field names.  Numeric columns are converted to float.
    """
    if columns is None:
        columns = [
            "name", "description", "logoid", "update_mode", "type", "typespecs",
            "close", "pricescale", "minmov", "fractional", "minmove2",
            "currency", "change", "volume", "relative_volume_10d_calc",
            "market_cap_basic", "fundamental_currency_code", "price_earnings_ttm",
            "earnings_per_share_diluted_ttm",
            "earnings_per_share_diluted_yoy_growth_ttm",
            "dividends_yield_current", "sector.tr", "market", "sector",
            "AnalystRating", "AnalystRating.tr",
        ]

    # Build (or reuse) the shared limiter
    rl = rate_limiter or make_shared_rate_limiter(
        name="tv-dse-markets",
        max_calls=2,
        per_seconds=1.0,
        burst=2,
        max_attempts=6,
        min_wait=1.0,
        max_wait=30.0,
        respect_retry_after=True,
    )

    api_url = "https://scanner.tradingview.com/bangladesh/scan"
    params = {"label-product": "markets-screener"}

    # Internal function for one API call, wrapped in the rate limiter
    @rl
    def _post(payload: Dict[str, any]) -> Dict[str, any]: # type: ignore
        resp = requests.post(
            api_url,
            params=params,
            json=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "text/plain;charset=UTF-8",
                "Origin": "https://www.tradingview.com",
                "Referer": "https://www.tradingview.com/",
                "DNT": "1",
            },
            timeout=30,
        )
        if resp.status_code == 429:
            # The rate limiter will catch this and back off
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()

    # Loop through pages
    all_rows: List[Dict[str, any]] = [] # type: ignore
    start = 0
    page = 1
    while True:
        end = start + page_size
        payload = {
            "columns": columns,
            "preset": "all_stocks",  # matches TradingView page preset
            "options": {"lang": "en"},
            "range": [start, end],
            "sort": {"sortBy": "name", "sortOrder": "asc", "nullsFirst": False},
            "ignore_unknown_fields": False,
        }
        t0 = time.time()
        response_json = _post(payload)
        rows = response_json.get("data", [])
        print(f"[TV-DSE] Page {page}: got {len(rows)} rows in {time.time() - t0:.2f}s")

        if not rows:
            break  # no more data
        all_rows.extend(rows)
        if len(rows) < page_size:
            break  # last page
        start += page_size
        page += 1

    # Convert to DataFrame
    # Each row looks like {"s": "DSEBD:ABC", "d": [...] }
    symbols = [r["s"] for r in all_rows]
    values = [r["d"] for r in all_rows]
    df = pd.DataFrame(values, columns=columns)
    df.insert(0, "symbol", symbols)

    # Coerce numeric columns to float
    numeric_fields = [
        "market_cap_basic", "close", "change", "volume",
        "relative_volume_10d_calc", "price_earnings_ttm",
        "earnings_per_share_diluted_ttm",
        "earnings_per_share_diluted_yoy_growth_ttm",
        "dividends_yield_current",
    ]
    for field in numeric_fields:
        if field in df.columns:
            s = pd.to_numeric(df[field], errors="coerce")
            df[field] = s.replace([np.inf, -np.inf], np.nan).astype(float) # type: ignore

    return df

def main(save_csv: bool = True) -> None:
    """
    Fetch DSE market movers using the TradingView markets-screener endpoint and
    optionally save the result to data/screener/.
    """
    df = fetch_dse_market_stock_df()
    print("\nPreview:")
    print(df.head().to_string(index=False))
    save_dse_data(df, save_as_csv=save_csv, _object="stock")

if __name__ == "__main__":
    main(save_csv=True)
