from __future__ import annotations

import pandas as pd

from bdi.metrics import emission_latency


def test_emission_latency_clamps_values_available_before_minute_end() -> None:
    candles = pd.DataFrame(
        {
            "minute": pd.to_datetime(
                ["2026-11-08T10:00:00Z", "2026-11-08T10:01:00Z"], utc=True
            ),
            "emitted_at": pd.to_datetime(
                ["2026-11-08T10:00:59Z", "2026-11-08T10:02:05Z"], utc=True
            ),
        }
    )

    assert emission_latency(candles).tolist() == [0.0, 5.0]
