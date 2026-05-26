"""Benchmark tickbar vs pandas — low-level throughput comparison."""
import time
import struct
import sys

import numpy as np
import pandas as pd
import yfinance as yf

from tickbar import Tick, TickAggregator

np.random.seed(42)


def download_data(tickers, period="5d", interval="1m"):
    """Download OHLCV data and expand into simulated ticks."""
    print(f"Downloading {interval} data for {len(tickers)} tickers ({period})...")
    data = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker",
        progress=False,
    )
    if len(tickers) == 1:
        data.columns = pd.MultiIndex.from_product([data.columns, tickers])

    timestamps = []
    prices_raw = []
    prices_fp = []
    volumes = []
    PRICE_SCALE = 100_000_000

    for t in tickers:
        try:
            df = data.xs(t, level=1, axis=1).dropna()
        except (KeyError, ValueError):
            try:
                df = data[t].dropna()
            except (KeyError, ValueError):
                print(f"  Skipping {t}: no data")
                continue

        for idx, row in df.iterrows():
            ts_ns = int(idx.timestamp() * 1_000_000_000)
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
            v = float(row["Volume"])
            if v <= 0:
                continue

            base_price = o
            for _ in range(4):
                jitter = float(np.random.uniform(-0.001, 0.001) * base_price)
                price_raw = base_price + jitter
                price_fp = int(round(price_raw * PRICE_SCALE))
                vol = v / 4 + float(np.random.uniform(-v / 8, v / 8))
                vol_int = int(max(vol, 1))
                t_ns = ts_ns + np.random.randint(0, 60_000_000_000)
                timestamps.append(t_ns)
                prices_raw.append(price_raw)
                prices_fp.append(price_fp)
                volumes.append(vol_int)

    order = np.argsort(timestamps)
    ts_arr = np.array(timestamps, dtype=np.int64)[order]
    pr_fp = np.array(prices_fp, dtype=np.int64)[order]
    pr_raw = np.array(prices_raw, dtype=np.float64)[order]
    vol_arr = np.array(volumes, dtype=np.int64)[order]
    return ts_arr, pr_fp, pr_raw, vol_arr


def bench(method, fn, desc):
    t0 = time.perf_counter()
    bars = fn()
    t1 = time.perf_counter()
    elapsed = t1 - t0
    if bars is not None:
        print(f"  {method:25s} {len(bars):>5} bars  {elapsed:>8.4f}s  ({desc})")
    else:
        print(f"  {method:25s}  {elapsed:>8.4f}s  ({desc})")
    return elapsed


def run_benchmarks(ts_arr, pr_fp, pr_raw, vol_arr):
    N = len(ts_arr)
    print(f"\n=== Real-world data ({N:,} ticks, 9 tickers, 5d) ===\n")

    # --- bytes path (zero-copy Rust) ---
    def pack_bytes():
        buf = bytearray(N * 32)
        for i in range(N):
            struct.pack_into("<qqqq", buf, i * 32, ts_arr[i], pr_fp[i], vol_arr[i], 0)
        return bytes(buf)

    packed = pack_bytes()

    def do_bytes():
        agg = TickAggregator(60)
        agg.push_from_bytes(packed)
        return agg.finalize()

    t_bytes = bench("tickbar (bytes)", do_bytes, f"{N/(0.001):,.0f} ticks baseline")

    # --- numpy path (zero-copy __array_interface__) ---
    def do_numpy():
        agg = TickAggregator(60)
        agg.push_from_numpy(ts_arr, pr_fp, vol_arr)
        return agg.finalize()

    t_numpy = bench("tickbar (numpy)", do_numpy, "zero-copy Rust")

    # --- arrays path (PyO3 Vec<i64> copy) ---
    def do_arrays():
        agg = TickAggregator(60)
        agg.push_from_arrays(
            ts_arr.tolist(), pr_fp.tolist(), vol_arr.tolist()
        )
        return agg.finalize()

    t_arrays = bench("tickbar (arrays)", do_arrays, "PyO3 Vec copy")

    # --- single-push path ---
    def do_single():
        agg = TickAggregator(60)
        for i in range(N):
            agg.push_tick(Tick(int(ts_arr[i]), float(pr_fp[i]), float(vol_arr[i])))
        return agg.finalize()

    t_single = bench("tickbar (single)", do_single, "FFI per tick")

    # --- pandas (resample only, pre-built DF) ---
    df = pd.DataFrame({
        "ts": pd.to_datetime(ts_arr, unit="ns"),
        "price": pr_raw,
        "volume": vol_arr,
    }).set_index("ts")

    def do_pandas_resample():
        resampled = df.resample("60s")
        return resampled.agg(
            open=("price", "first"), high=("price", "max"),
            low=("price", "min"), close=("price", "last"),
            volume=("volume", "sum"),
        ).dropna()

    t_pd_resamp = bench("pandas (resample)", do_pandas_resample, "Cython")

    # --- pandas (full: DF build + resample) ---
    def do_pandas_full():
        df2 = pd.DataFrame({
            "ts": pd.to_datetime(ts_arr, unit="ns"),
            "price": pr_raw, "volume": vol_arr,
        }).set_index("ts")
        res = df2.resample("60s").agg(
            open=("price", "first"), high=("price", "max"),
            low=("price", "min"), close=("price", "last"),
            volume=("volume", "sum"),
        ).dropna()
        return res

    t_pd_full = bench("pandas (full)", do_pandas_full, "incl DF build")

    print()
    print("=" * 62)
    print(f"{'Method':25s} {'ticks/s':>12s} {'vs native':>10s} {'vs pandas':>10s}")
    print("=" * 62)

    # Rough native Rust speed from Criterion: 1M in ~8.4ms = 119M ticks/s
    native_speed = 119_000_000
    base = t_bytes if t_bytes > 0 else 0.001
    print(f"{'tickbar bytes':25s} {N/base:>12,.0f} {N/base/native_speed*100:>9.1f}% {t_pd_resamp/base:>9.1f}x")
    base = t_numpy if t_numpy > 0 else 0.001
    print(f"{'tickbar numpy':25s} {N/base:>12,.0f} {N/base/native_speed*100:>9.1f}% {t_pd_resamp/base:>9.1f}x")
    base = t_arrays if t_arrays > 0 else 0.001
    print(f"{'tickbar arrays':25s} {N/base:>12,.0f} {N/base/native_speed*100:>9.1f}% {t_pd_resamp/base:>9.1f}x")
    base = t_single if t_single > 0 else 0.001
    print(f"{'tickbar single':25s} {N/base:>12,.0f} {N/base/native_speed*100:>9.1f}% {t_pd_resamp/base:>9.1f}x")
    base = t_pd_resamp if t_pd_resamp > 0 else 0.001
    print(f"{'pandas resample':25s} {N/base:>12,.0f} {'N/A':>10s} {'1.0x':>10s}")
    base = t_pd_full if t_pd_full > 0 else 0.001
    print(f"{'pandas full':25s} {N/base:>12,.0f} {'N/A':>10s} {t_pd_resamp/base:>9.1f}x")
    print("=" * 62)


if __name__ == "__main__":
    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "JPM", "V", "WMT"]
    ts_arr, pr_fp, pr_raw, vol_arr = download_data(tickers, period="5d", interval="1m")
    run_benchmarks(ts_arr, pr_fp, pr_raw, vol_arr)

    # --- Synthetic 1M tick benchmark (apples-to-apples with Criterion) ---
    print("\n\n=== Synthetic 1M ticks (matching Criterion bench) ===\n")
    N1M = 1_000_000
    ts_1m = np.arange(N1M, dtype=np.int64) * 1_000_000_000
    pr_1m = np.full(N1M, 100_000_000, dtype=np.int64) + (np.arange(N1M) % 1000) * 1000
    vol_1m = np.full(N1M, 1_000_000, dtype=np.int64)

    def do_numpy_1m():
        agg = TickAggregator(60)
        agg.push_from_numpy(ts_1m, pr_1m, vol_1m)
        return agg.finalize()

    t = bench("tickbar numpy 1M", do_numpy_1m, f"{N1M/(0.001):,.0f} ticks")
    print(f"\n  →  {N1M/t:,.0f} ticks/s  ({t*1000:.2f}ms for {N1M:,} ticks)")
    print(f"  →  {N1M/t/119_000_000*100:.1f}% of native Rust speed (119M ticks/s)")
