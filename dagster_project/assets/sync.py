from datetime import date

import pyarrow.dataset as ds
from dagster import AssetDep, AssetExecutionContext, MetadataValue, RetryPolicy, asset
from psycopg2.extras import execute_values

from dagster_project.partitions import daily_partitions, weekly_partitions
from dagster_project.resources import MinioResource, PostgresResource


def _day_filter(partition_key: str):
    d = date.fromisoformat(partition_key)
    return (
        (ds.field("year")  == d.strftime("%Y")) &
        (ds.field("month") == d.strftime("%m")) &
        (ds.field("day")   == d.strftime("%d"))
    )


def _str(val):
    return str(val)[:10] if val is not None else None


def _float(val):
    return float(val) if val is not None else None


def _int(val):
    return int(val) if val is not None else None


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("ohlcv_daily_bars")],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    group_name="serving_layer",
    description="Syncs OHLCV bars and symbol catalog from MinIO Parquet into Postgres.",
    metadata={"tables": MetadataValue.text("ohlcv_bars, symbols")},
)
def ohlcv_pg_sync(
    context: AssetExecutionContext,
    minio: MinioResource,
    pg: PostgresResource,
) -> None:
    table = minio.read_parquet(
        minio.market_analysis_bucket, "ohlcv.bar", _day_filter(context.partition_key)
    )
    if len(table) == 0:
        context.log.warning("No OHLCV rows for %s — skipping sync", context.partition_key)
        return

    bar_rows = [
        (
            row["symbol"],
            row["asset_class"],
            row["exchange"],
            context.partition_key,  # use partition date — checkpoint skew prevention
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            int(row["volume"]),
        )
        for row in table.to_pylist()
    ]

    symbol_rows = {}
    for r in bar_rows:
        sym, ac, ex, t = r[0], r[1], r[2], r[3]
        key = (sym, ac)
        if key not in symbol_rows:
            symbol_rows[key] = [sym, ac, ex, t, t]
        else:
            symbol_rows[key][3] = min(symbol_rows[key][3], t)
            symbol_rows[key][4] = max(symbol_rows[key][4], t)

    with pg.connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO ohlcv_bars (symbol, asset_class, exchange, time, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, time) DO UPDATE SET
                    asset_class = EXCLUDED.asset_class,
                    exchange    = EXCLUDED.exchange,
                    open        = EXCLUDED.open,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close       = EXCLUDED.close,
                    volume      = EXCLUDED.volume
            """, bar_rows)

            execute_values(cur, """
                INSERT INTO symbols (symbol, asset_class, exchange, first_date, last_date)
                VALUES %s
                ON CONFLICT (symbol, asset_class) DO UPDATE SET
                    exchange   = EXCLUDED.exchange,
                    first_date = LEAST(symbols.first_date, EXCLUDED.first_date),
                    last_date  = GREATEST(symbols.last_date, EXCLUDED.last_date),
                    updated_at = now()
            """, [tuple(v) for v in symbol_rows.values()])

        conn.commit()

    context.log.info("Synced %d OHLCV bars and %d symbols for %s",
                     len(bar_rows), len(symbol_rows), context.partition_key)
    context.add_output_metadata({"rows_synced": len(bar_rows)})


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("technical_indicators")],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    group_name="serving_layer",
    description="Syncs technical indicators from MinIO Parquet into Postgres.",
    metadata={"table": MetadataValue.text("technical_indicators")},
)
def indicators_pg_sync(
    context: AssetExecutionContext,
    minio: MinioResource,
    pg: PostgresResource,
) -> None:
    table = minio.read_parquet(
        minio.market_analysis_bucket, "technical.indicators", _day_filter(context.partition_key)
    )
    if len(table) == 0:
        context.log.warning("No indicator rows for %s — skipping sync", context.partition_key)
        return

    rows = [
        (
            row["symbol"],
            context.partition_key,  # use partition date — Spark checkpoint skews row["time"]
            float(row["close"]),
            _float(row.get("sma20")),
            _float(row.get("sma50")),
            _float(row.get("sma200")),
            _float(row.get("rsi14")),
            _float(row.get("macd")),
            _float(row.get("macd_signal")),
            _float(row.get("macd_hist")),
            _float(row.get("bb_upper")),
            _float(row.get("bb_mid")),
            _float(row.get("bb_lower")),
        )
        for row in table.to_pylist()
    ]

    with pg.connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO technical_indicators
                    (symbol, time, close, sma20, sma50, sma200, rsi14,
                     macd, macd_signal, macd_hist, bb_upper, bb_mid, bb_lower)
                VALUES %s
                ON CONFLICT (symbol, time) DO UPDATE SET
                    close       = EXCLUDED.close,
                    sma20       = EXCLUDED.sma20,
                    sma50       = EXCLUDED.sma50,
                    sma200      = EXCLUDED.sma200,
                    rsi14       = EXCLUDED.rsi14,
                    macd        = EXCLUDED.macd,
                    macd_signal = EXCLUDED.macd_signal,
                    macd_hist   = EXCLUDED.macd_hist,
                    bb_upper    = EXCLUDED.bb_upper,
                    bb_mid      = EXCLUDED.bb_mid,
                    bb_lower    = EXCLUDED.bb_lower
            """, rows)
        conn.commit()

    context.log.info("Synced %d indicator rows for %s", len(rows), context.partition_key)
    context.add_output_metadata({"rows_synced": len(rows)})


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("daily_digest")],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    group_name="serving_layer",
    description="Syncs daily digest rankings from MinIO Parquet into Postgres.",
    metadata={"table": MetadataValue.text("digest_entries")},
)
def digest_pg_sync(
    context: AssetExecutionContext,
    minio: MinioResource,
    pg: PostgresResource,
) -> None:
    d = date.fromisoformat(context.partition_key)
    filter_expr = (
        (ds.field("year")  == d.strftime("%Y")) &
        (ds.field("month") == d.strftime("%m")) &
        (ds.field("day")   == d.strftime("%d"))
    )
    table = minio.read_parquet(minio.market_analysis_bucket, "digest", filter_expr)
    if len(table) == 0:
        context.log.warning("No digest rows for %s — skipping sync", context.partition_key)
        return

    rows = [
        (
            context.partition_key,
            row["category"],
            int(row["rank"]),
            row["symbol"],
            row["exchange"],
            row["asset_class"],
            float(row["open"]),
            float(row["close"]),
            int(row["volume"]),
            float(row["pct_change"]),
        )
        for row in table.to_pylist()
    ]

    with pg.connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO digest_entries
                    (date, category, rank, symbol, exchange, asset_class,
                     open, close, volume, pct_change)
                VALUES %s
                ON CONFLICT (date, category, rank) DO UPDATE SET
                    symbol      = EXCLUDED.symbol,
                    exchange    = EXCLUDED.exchange,
                    asset_class = EXCLUDED.asset_class,
                    open        = EXCLUDED.open,
                    close       = EXCLUDED.close,
                    volume      = EXCLUDED.volume,
                    pct_change  = EXCLUDED.pct_change
            """, rows)
        conn.commit()

    context.log.info("Synced %d digest entries for %s", len(rows), context.partition_key)
    context.add_output_metadata({"rows_synced": len(rows)})


@asset(
    partitions_def=weekly_partitions,
    deps=[AssetDep("screener_results")],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    group_name="serving_layer",
    description="Syncs weekly screener fundamentals from MinIO Parquet into Postgres.",
    metadata={"table": MetadataValue.text("screener_results")},
)
def screener_pg_sync(
    context: AssetExecutionContext,
    minio: MinioResource,
    pg: PostgresResource,
) -> None:
    d = date.fromisoformat(context.partition_key)
    iso_year, iso_week, _ = d.isocalendar()
    year = str(iso_year)
    week = f"{iso_week:02d}"

    filter_expr = (ds.field("year") == year) & (ds.field("week") == week)
    table = minio.read_parquet(minio.market_analysis_bucket, "screener", filter_expr)
    if len(table) == 0:
        context.log.warning("No screener rows for week %s-%s — skipping sync", year, week)
        return

    rows = [
        (
            year,
            week,
            row["symbol"],
            _float(row.get("pe_ratio")),
            _float(row.get("pb_ratio")),
            _float(row.get("roe")),
            _float(row.get("eps")),
            _float(row.get("de_ratio")),
            _float(row.get("current_ratio")),
        )
        for row in table.to_pylist()
    ]

    with pg.connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO screener_results
                    (year, week, symbol, pe_ratio, pb_ratio, roe, eps, de_ratio, current_ratio)
                VALUES %s
                ON CONFLICT (year, week, symbol) DO UPDATE SET
                    pe_ratio      = EXCLUDED.pe_ratio,
                    pb_ratio      = EXCLUDED.pb_ratio,
                    roe           = EXCLUDED.roe,
                    eps           = EXCLUDED.eps,
                    de_ratio      = EXCLUDED.de_ratio,
                    current_ratio = EXCLUDED.current_ratio
            """, rows)
        conn.commit()

    context.log.info("Synced %d screener rows for week %s-%s", len(rows), year, week)
    context.add_output_metadata({"rows_synced": len(rows)})
