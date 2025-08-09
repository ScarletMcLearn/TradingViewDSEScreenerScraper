# # from scripts.utils import *
# from utils import *
# import pandas as pd
# import time

# # Step 1: build a shared rate limiter
# rate_limiter = make_shared_rate_limiter(
#     name="tradingview-dse",
#     max_calls=2,         # two requests per second
#     per_seconds=1.0,
#     burst=2,             # allow short bursts of two
#     max_attempts=6,      # retry up to 6 times on HTTP 429
#     min_wait=1.0,
#     max_wait=30.0,
#     respect_retry_after=True,
# )

# # Step 2: define the fields you want to collect
# cols = [
#     "name",                          # ticker symbol
#     "description",                   # company description
#     "market_cap_basic",              # market cap
#     "close",                         # last close price
#     "change",                        # daily % change
#     "volume",                        # trading volume
#     "relative_volume_10d_calc",      # relative volume
#     "price_earnings_ttm",            # P/E ratio
#     "earnings_per_share_diluted_ttm",# EPS (diluted, TTM)
#     "dividends_yield_current",       # dividend yield %
#     "sector.tr"                      # sector (translated)
# ]

# # Step 3: page through results (100 rows at a time)
# all_rows = []
# start = 0
# page_size = 100
# page_num = 1

# print("=== Starting DSE TradingView data retrieval ===")
# while True:
#     end = start + page_size
#     print(f"[Page {page_num}] Fetching rows {start}–{end} ...", flush=True)

#     start_time = time.time()
#     result = tradingview_scan_bangladesh(
#         columns=cols,
#         result_range=(start, end),
#         sort_by="market_cap",
#         sort_order="desc",
#         rate_limiter=rate_limiter
#     )
#     elapsed = time.time() - start_time

#     rows = result["data"]
#     print(f"[Page {page_num}] Retrieved {len(rows)} rows in {elapsed:.2f}s", flush=True)

#     if not rows:
#         print("[Info] No more rows returned — stopping.")
#         break

#     all_rows.extend(rows)
#     if len(rows) < page_size:
#         print("[Info] Last page reached.")
#         break

#     start += page_size
#     page_num += 1

# print(f"=== Retrieval complete: {len(all_rows)} total rows ===")

# # Step 4: convert to DataFrame
# symbols = [r["s"] for r in all_rows]
# data_values = [r["d"] for r in all_rows]
# df = pd.DataFrame(data_values, columns=cols)
# df.insert(0, "symbol", symbols)

# print("\nSample data:")
# # print(df.head(10).to_string(index=False))


# import numpy as np
# import pandas as pd

# num_cols = [
#     "market_cap_basic","close","change","volume","relative_volume_10d_calc",
#     "price_earnings_ttm","earnings_per_share_diluted_ttm","dividends_yield_current"
# ]

# for c in num_cols:
#     if c in df.columns:
#         s = pd.to_numeric(df[c], errors="coerce")
#         # IMPORTANT: use np.nan (not pd.NA) and lock dtype to float
#         s = s.replace([np.inf, -np.inf], np.nan).astype(float)
#         df[c] = s

# # optional, nicer formatting
# pd.set_option("display.float_format", lambda x: f"{x:.4f}")


# df.head()


# # Example: save the DataFrame if the flag is True
# save_dse_screener(df, save_as_csv=True)


# scripts/fetch_dse.py
from scripts.utils import *  # keep this since Actions runs from repo root
import pandas as pd
import numpy as np
import time

def fetch_dse_dataframe(rate_limiter=None) -> pd.DataFrame:
    # Step 1: shared rate limiter (use provided or build one)
    rl = rate_limiter or make_shared_rate_limiter(
        name="tradingview-dse",
        max_calls=2,
        per_seconds=1.0,
        burst=2,
        max_attempts=6,
        min_wait=1.0,
        max_wait=30.0,
        respect_retry_after=True,
    )

    # Step 2: fields
    cols = [
        "name",
        "description",
        "market_cap_basic",
        "close",
        "change",
        "volume",
        "relative_volume_10d_calc",
        "price_earnings_ttm",
        "earnings_per_share_diluted_ttm",
        "dividends_yield_current",
        "sector.tr",
    ]

    # Step 3: pagination
    all_rows = []
    start = 0
    page_size = 100
    page_num = 1

    print("=== Starting DSE TradingView data retrieval ===", flush=True)
    while True:
        end = start + page_size
        print(f"[Page {page_num}] Fetching rows {start}–{end} ...", flush=True)

        t0 = time.time()
        result = tradingview_scan_bangladesh(
            columns=cols,
            result_range=(start, end),
            sort_by="market_cap",
            sort_order="desc",
            rate_limiter=rl,
        )
        dt = time.time() - t0
        rows = result["data"]
        print(f"[Page {page_num}] Retrieved {len(rows)} rows in {dt:.2f}s", flush=True)

        if not rows:
            print("[Info] No more rows returned — stopping.", flush=True)
            break

        all_rows.extend(rows)
        if len(rows) < page_size:
            print("[Info] Last page reached.", flush=True)
            break

        start += page_size
        page_num += 1

    print(f"=== Retrieval complete: {len(all_rows)} total rows ===", flush=True)

    # Step 4: to DataFrame
    symbols = [r["s"] for r in all_rows]
    data_values = [r["d"] for r in all_rows]
    df = pd.DataFrame(data_values, columns=cols)
    df.insert(0, "symbol", symbols)

    # Clean numeric columns
    num_cols = [
        "market_cap_basic","close","change","volume","relative_volume_10d_calc",
        "price_earnings_ttm","earnings_per_share_diluted_ttm","dividends_yield_current"
    ]
    for c in num_cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            s = s.replace([np.inf, -np.inf], np.nan).astype(float) # type: ignore
            df[c] = s

    pd.set_option("display.float_format", lambda x: f"{x:.4f}")
    return df

def main(save_csv: bool = True) -> None:
    df = fetch_dse_dataframe()
    print("\nSample data:")
    print(df.head(10).to_string(index=False))
    # Save via your util
    save_dse_screener(df, save_as_csv=save_csv, _object="screener")

if __name__ == "__main__":
    # When run directly (e.g. locally)
    main(save_csv=True)
