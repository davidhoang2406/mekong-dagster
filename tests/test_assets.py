from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from dagster_project.assets.ohlcv import ohlcv_daily_bars
from dagster_project.assets.technical import technical_indicators
from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


# ── ohlcv_daily_bars ──────────────────────────────────────────────────────────

@patch.object(SparkClusterResource, "submit")
def test_ohlcv_calls_submit_with_date(mock_submit):
    spark = SparkClusterResource(container_name="spark-master")
    ctx   = build_asset_context(partition_key="2026-05-16")
    ohlcv_daily_bars(ctx, spark=spark)
    mock_submit.assert_called_once_with(["ohlcv-daily-ingest", "--date", "2026-05-16"])


@patch.object(SparkClusterResource, "submit")
def test_ohlcv_partition_key_propagates(mock_submit):
    spark = SparkClusterResource(container_name="spark-master")
    ctx   = build_asset_context(partition_key="2026-05-01")
    ohlcv_daily_bars(ctx, spark=spark)
    mock_submit.assert_called_once_with(["ohlcv-daily-ingest", "--date", "2026-05-01"])


# ── technical_indicators ──────────────────────────────────────────────────────

@patch.object(SparkClusterResource, "submit")
def test_technical_calls_submit(mock_submit):
    spark = SparkClusterResource(container_name="spark-master")
    ctx   = build_asset_context(partition_key="2026-05-16")
    technical_indicators(ctx, spark=spark)
    mock_submit.assert_called_once_with(["technical"])


# ── SparkClusterResource ──────────────────────────────────────────────────────

def test_spark_resource_default_container():
    assert SparkClusterResource(container_name="spark-master").container_name == "spark-master"


def test_spark_submit_raises_on_nonzero(monkeypatch):
    import docker as docker_sdk
    mock_container = MagicMock()
    mock_container.exec_run.return_value = (1, b"job failed")
    mock_client = MagicMock()
    mock_client.containers.get.return_value = mock_container
    monkeypatch.setattr(docker_sdk, "from_env", lambda: mock_client)
    with pytest.raises(RuntimeError, match="Spark job failed"):
        SparkClusterResource(container_name="spark-master").submit(["ohlcv-daily-ingest", "--date", "2026-05-16"])


# ── Partitions ────────────────────────────────────────────────────────────────

def test_daily_partitions_timezone():
    assert daily_partitions.timezone == "Asia/Ho_Chi_Minh"


def test_daily_partitions_includes_today():
    keys = daily_partitions.get_partition_keys()
    assert "2026-05-01" in keys
    assert "2026-05-17" in keys


# ── Definitions smoke test ────────────────────────────────────────────────────

def test_definitions_load():
    from dagster_project import defs
    assert defs is not None
