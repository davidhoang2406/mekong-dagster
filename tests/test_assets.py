from unittest.mock import ANY, MagicMock, patch

import pytest
from dagster import build_asset_context

from dagster_project.assets.ohlcv import ohlcv_daily_bars
from dagster_project.assets.technical import technical_indicators
from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


# ── ohlcv_daily_bars ──────────────────────────────────────────────────────────

@patch.object(SparkClusterResource, "submit")
def test_ohlcv_calls_submit_with_date(mock_submit):
    spark = SparkClusterResource()
    ctx   = build_asset_context(partition_key="2026-05-16")
    ohlcv_daily_bars(ctx, spark=spark)
    mock_submit.assert_called_once_with(["ohlcv-daily-ingest", "--date", "2026-05-16"], logger=ANY)


@patch.object(SparkClusterResource, "submit")
def test_ohlcv_partition_key_propagates(mock_submit):
    spark = SparkClusterResource()
    ctx   = build_asset_context(partition_key="2026-05-01")
    ohlcv_daily_bars(ctx, spark=spark)
    mock_submit.assert_called_once_with(["ohlcv-daily-ingest", "--date", "2026-05-01"], logger=ANY)


# ── technical_indicators ──────────────────────────────────────────────────────

@patch.object(SparkClusterResource, "submit")
def test_technical_calls_submit(mock_submit):
    spark = SparkClusterResource()
    ctx   = build_asset_context(partition_key="2026-05-16")
    technical_indicators(ctx, spark=spark)
    mock_submit.assert_called_once_with(["technical", "--date", "2026-05-16"], logger=ANY)


# ── SparkClusterResource ──────────────────────────────────────────────────────

def test_spark_resource_defaults():
    spark = SparkClusterResource()
    assert spark.namespace == "mekong-processing"
    assert spark.service_account == "spark"
    assert "mekong-spark" in spark.spark_image


def test_spark_submit_raises_on_failed_state(monkeypatch):
    """submit() must raise RuntimeError when SparkApplication reaches FAILED state."""
    mock_custom_api = MagicMock()
    mock_custom_api.create_namespaced_custom_object.return_value = {}
    mock_custom_api.get_namespaced_custom_object.return_value = {
        "status": {"applicationState": {"state": "FAILED", "errorMessage": "OOM"}}
    }

    mock_k8s_client = MagicMock()
    mock_k8s_client.CustomObjectsApi.return_value = mock_custom_api

    with patch("dagster_project.resources._k8s", return_value=mock_k8s_client), \
         patch("time.sleep"):
        with pytest.raises(RuntimeError, match="FAILED"):
            SparkClusterResource().submit(["ohlcv-daily-ingest", "--date", "2026-05-16"])


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
