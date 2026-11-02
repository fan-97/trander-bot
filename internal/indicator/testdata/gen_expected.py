"""Independent reference for indicator tests (TradingView / Pine v5 conventions).
Usage: python3 -I gen_expected.py btcusdt_1h.csv tv_expected.csv
"""
import csv
import math
import sys


def sma_at(src, i, n):
    # src entries may be None; defined only if the full window is defined
    w = src[i - n + 1:i + 1] if i >= n - 1 else None
    if w is None or any(v is None for v in w):
        return None
    return sum(w) / n


def seeded(src, n, alpha):
    """Pine ema/rma: seed = SMA of first n defined values, then recursive."""
    out = [None] * len(src)
    first = next((i for i, v in enumerate(src) if v is not None), None)
    if first is None:
        return out
    for i in range(first + n - 1, len(src)):
        if i == first + n - 1:
            out[i] = sum(src[first:first + n]) / n
        else:
            out[i] = alpha * src[i] + (1 - alpha) * out[i - 1]
    return out


def ema(src, n):
    return seeded(src, n, 2.0 / (n + 1))


def rma(src, n):
    return seeded(src, n, 1.0 / n)


def main(inp, outp):
    rows = list(csv.DictReader(open(inp)))
    ts = [r["open_time_ms"] for r in rows]
    hi = [float(r["high"]) for r in rows]
    lo = [float(r["low"]) for r in rows]
    cl = [float(r["close"]) for r in rows]
    N = len(rows)

    # RSI(14): changes start at bar 1
    up = [None] + [max(cl[i] - cl[i - 1], 0.0) for i in range(1, N)]
    dn = [None] + [-min(cl[i] - cl[i - 1], 0.0) for i in range(1, N)]
    ru, rd = rma(up, 14), rma(dn, 14)
    rsi = [None] * N
    for i in range(N):
        if ru[i] is None:
            continue
        if rd[i] == 0:
            rsi[i] = 100.0
        elif ru[i] == 0:
            rsi[i] = 0.0
        else:
            rsi[i] = 100.0 - 100.0 / (1.0 + ru[i] / rd[i])

    e12, e26 = ema(cl, 12), ema(cl, 26)
    macd = [None if a is None or b is None else a - b for a, b in zip(e12, e26)]
    sig = ema(macd, 9)
    hist = [None if m is None or s is None else m - s for m, s in zip(macd, sig)]

    bu, bm, bl = [None] * N, [None] * N, [None] * N
    for i in range(19, N):
        w = cl[i - 19:i + 1]
        mid = sum(w) / 20
        dev = math.sqrt(sum((x - mid) ** 2 for x in w) / 20)
        bm[i], bu[i], bl[i] = mid, mid + 2 * dev, mid - 2 * dev

    tr = []
    for i in range(N):
        if i == 0:
            tr.append(hi[i] - lo[i])
        else:
            tr.append(max(hi[i] - lo[i], abs(hi[i] - cl[i - 1]), abs(lo[i] - cl[i - 1])))
    atr = rma(tr, 14)

    cols = [rsi, e12, e26, macd, sig, hist, bu, bm, bl, atr]
    names = ["rsi14", "ema12", "ema26", "macd", "macd_signal", "macd_hist",
             "bb_upper", "bb_mid", "bb_lower", "atr14"]
    with open(outp, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["open_time_ms"] + names)
        for i in range(N):
            w.writerow([ts[i]] + ["" if c[i] is None else repr(c[i]) for c in cols])


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
