-- Khởi tạo Postgres (chạy tự động khi container postgres khởi động lần đầu).

-- C6: bảng nến chung cho 3 nguồn, khóa idempotent (symbol, minute, source)
-- source: 'official' | 'batch' | 'stream' | 'stream:wm=5s' (các lần chạy E3) ...
CREATE TABLE IF NOT EXISTS candles (
    symbol            text        NOT NULL,
    minute            timestamptz NOT NULL,
    source            text        NOT NULL,
    open              numeric(24, 8),
    high              numeric(24, 8),
    low               numeric(24, 8),
    close             numeric(24, 8),
    volume            numeric(38, 8),
    quote_volume      numeric(38, 16),
    trades            bigint,
    n_agg             bigint,
    first_agg_id      bigint,
    last_agg_id       bigint,
    -- lineage
    file_checksum     text,
    ingested_at       timestamptz,
    watermark_config  text,
    pipeline_version  text,
    emitted_at        timestamptz DEFAULT now(),   -- lúc ghi vào DB (đo độ trễ E3)
    PRIMARY KEY (symbol, minute, source)
);
CREATE INDEX IF NOT EXISTS candles_minute_idx ON candles (minute);

-- C5: mọi phiên bản của nến streaming (output_mode=update) để tính time-to-convergence
CREATE TABLE IF NOT EXISTS candle_versions (
    symbol      text        NOT NULL,
    minute      timestamptz NOT NULL,
    source      text        NOT NULL,
    batch_id    bigint      NOT NULL,
    open        numeric(24, 8),
    high        numeric(24, 8),
    low         numeric(24, 8),
    close       numeric(24, 8),
    volume      numeric(38, 8),
    trades      bigint,
    emitted_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, minute, source, batch_id)
);

-- E4: trạng thái bộ nhận WS — aggId cuối cùng đã đẩy vào Kafka (để backfill)
CREATE TABLE IF NOT EXISTS ingest_state (
    symbol       text PRIMARY KEY,
    last_agg_id  bigint NOT NULL,
    last_ts_us   bigint,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- E4 / C3: nhật ký khoảng trống do mất kết nối WS (dùng gắn nhãn DISCONNECT_GAP)
CREATE TABLE IF NOT EXISTS ws_gaps (
    id                bigserial PRIMARY KEY,
    symbol            text        NOT NULL,
    disconnected_at   timestamptz NOT NULL,
    reconnected_at    timestamptz,
    last_agg_id       bigint,          -- aggId cuối trước khi ngắt
    resumed_agg_id    bigint,          -- aggId đầu tiên nhận lại qua WS
    backfilled        bigint DEFAULT 0,
    reason            text
);

-- E4: nhật ký sự cố do nhóm chủ động gây ra (chaos scripts ghi vào)
CREATE TABLE IF NOT EXISTS chaos_events (
    id          bigserial PRIMARY KEY,
    scenario    text        NOT NULL,
    phase       text        NOT NULL,   -- inject | recover
    at          timestamptz NOT NULL DEFAULT now(),
    note        text
);

-- C3: kết quả đối soát từng nến
CREATE TABLE IF NOT EXISTS reconcile_results (
    run_id      text        NOT NULL,
    symbol      text        NOT NULL,
    minute      timestamptz NOT NULL,
    left_src    text        NOT NULL,
    right_src   text        NOT NULL,
    match_open  boolean, match_high boolean, match_low boolean, match_close boolean,
    match_volume boolean, match_trades boolean,
    all_match   boolean,
    cause       text,
    PRIMARY KEY (run_id, symbol, minute, left_src, right_src)
);

-- E7: cảnh báo bất thường
CREATE TABLE IF NOT EXISTS anomalies (
    symbol      text        NOT NULL,
    minute      timestamptz NOT NULL,
    metric      text        NOT NULL,   -- log_return | volume
    value       double precision,
    mz_score    double precision,
    source      text        NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, minute, metric, source)
);
