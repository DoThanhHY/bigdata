"""C4 (linkage) + E7 (modified z-score)."""

from decimal import Decimal as D

import numpy as np
import pandas as pd

from bdi.anomaly.mzscore import rolling_mz
from bdi.linkage.fuzzy import STRATEGIES, link, prepare, score


def _data():
    rng = np.random.default_rng(0)
    n = 400
    ts = np.sort(rng.integers(0, 60_000_000, n)) + 1_786_795_200_000_000
    price = [D("100.00") + D(int(x)) / 100 for x in rng.integers(0, 50, n)]
    qty = [D(int(x)) / 1000 for x in rng.integers(1, 20, n)]
    batch = pd.DataFrame({"symbol": "BTCUSDT", "ts_us": ts, "price": price, "qty": qty, "agg_id": np.arange(n)})
    # stream = batch + độ trễ mạng 5..80ms, mất 5% bản tin
    keep = rng.random(n) > 0.05
    stream = batch[keep].assign(ts_us=lambda d: d["ts_us"] + rng.integers(5_000, 80_000, len(d)))
    return prepare(stream, "s"), prepare(batch, "b")


def test_linkage_scores_and_blocking_consistent():
    s, b = _data()
    res = {}
    for strat in STRATEGIES:
        m, n_cmp = link(s, b, 100_000, strat, price_bucket=0.1)
        res[strat] = (score(m, s, b), n_cmp)
    sc = res["second"][0]
    assert sc["recall"] > 0.95 and sc["precision"] > 0.95
    # cùng δ => cùng kết quả ghép; blocking chỉ đổi chi phí
    assert len({r[0]["tp"] for r in res.values()}) == 1
    assert res["none"][1] > res["second"][1] > res["second_price"][1]


def test_small_delta_hurts_recall():
    s, b = _data()
    low = score(link(s, b, 10_000)[0], s, b)["recall"]
    high = score(link(s, b, 100_000)[0], s, b)["recall"]
    assert low < high


def test_mzscore_flags_spike():
    x = pd.Series(np.r_[np.random.default_rng(1).normal(0, 1, 200), 25.0])
    mz = rolling_mz(x, 60)
    assert abs(mz.iloc[-1]) > 3.5
    assert mz.iloc[:60].isna().all()          # chưa đủ lịch sử => không chấm điểm
    assert (mz.iloc[150:200].abs() < 6).all()  # nhiễu chuẩn không bị gắn cờ mạnh
