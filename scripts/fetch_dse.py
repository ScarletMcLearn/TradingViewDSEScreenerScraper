from scripts.utils import *
import pandas as pd
import time

# Step 1: build a shared rate limiter
rate_limiter = make_shared_rate_limiter(
    name="tradingview-dse",
    max_calls=2,         # two requests per second
    per_seconds=1.0,
    burst=2,             # allow short bursts of two
    max_attempts=6,      # retry up to 6 times on HTTP 429
    min_wait=1.0,
    max_wait=30.0,
    respect_retry_after=True,
)

# Step 2: define the fields you want to collect
cols = [
    "name",                          # ticker symbol
    "description",                   # company description
    "market_cap_basic",              # market cap
    "close",                         # last close price
    "change",                        # daily % change
    "volume",                        # trading volume
    "relative_volume_10d_calc",      # relative volume
    "price_earnings_ttm",            # P/E ratio
    "earnings_per_share_diluted_ttm",# EPS (diluted, TTM)
    "dividends_yield_current",       # dividend yield %
    "sector.tr"                      # sector (translated)
]

# Step 3: page through results (100 rows at a time)
all_rows = []
start = 0
page_size = 100
page_num = 1

print("=== Starting DSE TradingView data retrieval ===")
while True:
    end = start + page_size
    print(f"[Page {page_num}] Fetching rows {start}–{end} ...", flush=True)

    start_time = time.time()
    result = tradingview_scan_bangladesh(
        columns=cols,
        result_range=(start, end),
        sort_by="market_cap",
        sort_order="desc",
        rate_limiter=rate_limiter
    )
    elapsed = time.time() - start_time

    rows = result["data"]
    print(f"[Page {page_num}] Retrieved {len(rows)} rows in {elapsed:.2f}s", flush=True)

    if not rows:
        print("[Info] No more rows returned — stopping.")
        break

    all_rows.extend(rows)
    if len(rows) < page_size:
        print("[Info] Last page reached.")
        break

    start += page_size
    page_num += 1

print(f"=== Retrieval complete: {len(all_rows)} total rows ===")

# Step 4: convert to DataFrame
symbols = [r["s"] for r in all_rows]
data_values = [r["d"] for r in all_rows]
df = pd.DataFrame(data_values, columns=cols)
df.insert(0, "symbol", symbols)

print("\nSample data:")
# print(df.head(10).to_string(index=False))


import numpy as np
import pandas as pd

num_cols = [
    "market_cap_basic","close","change","volume","relative_volume_10d_calc",
    "price_earnings_ttm","earnings_per_share_diluted_ttm","dividends_yield_current"
]

for c in num_cols:
    if c in df.columns:
        s = pd.to_numeric(df[c], errors="coerce")
        # IMPORTANT: use np.nan (not pd.NA) and lock dtype to float
        s = s.replace([np.inf, -np.inf], np.nan).astype(float)
        df[c] = s

# optional, nicer formatting
pd.set_option("display.float_format", lambda x: f"{x:.4f}")


df.head()


# Example: save the DataFrame if the flag is True
save_dse_screener(df, save_as_csv=True)