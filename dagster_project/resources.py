import logging
from typing import Optional

import docker as docker_sdk
from dagster import ConfigurableResource
from minio import Minio

log = logging.getLogger(__name__)


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    market_data_bucket: str
    market_analysis_bucket: str

    def _client(self) -> Minio:
        secure = self.endpoint.startswith("https://")
        host   = self.endpoint.split("://", 1)[-1]
        return Minio(host, access_key=self.access_key, secret_key=self.secret_key, secure=secure)

    def partition_exists(self, bucket: str, prefix: str) -> bool:
        try:
            return any(True for _ in self._client().list_objects(bucket, prefix=prefix, recursive=False))
        except Exception:
            return False

    def object_exists(self, bucket: str, key: str) -> bool:
        from minio.error import S3Error
        try:
            self._client().stat_object(bucket, key)
            return True
        except S3Error:
            return False
        except Exception:
            return False

    def read_parquet(self, bucket: str, dataset_path: str, filter_expr=None) -> "pa.Table":
        """Read a Hive-partitioned Parquet dataset from MinIO via s3fs + pyarrow.

        dataset_path is the prefix below the bucket root, e.g. "ohlcv.bar".
        filter_expr is an optional pyarrow.dataset Expression for partition pruning.
        """
        import pyarrow as pa  # noqa: F401
        import pyarrow.dataset as ds
        import s3fs

        fs = s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            endpoint_url=self.endpoint,
            use_ssl=self.endpoint.startswith("https://"),
        )
        dataset = ds.dataset(
            f"{bucket}/{dataset_path}",
            filesystem=fs,
            format="parquet",
            partitioning="hive",
        )
        return dataset.to_table(filter=filter_expr)


class KafkaAdminResource(ConfigurableResource):
    """Checks Kafka consumer group lag via KafkaAdminClient."""
    bootstrap_servers: str

    def consumer_group_lag(self, group_id: str) -> int:
        """Total uncommitted messages across all partitions for group_id.

        Returns -1 when the group has no committed offsets (not yet started or
        was reset). Returns 0 when fully caught up.
        """
        from kafka import KafkaAdminClient, KafkaConsumer

        admin = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="dagster-lag-check",
            request_timeout_ms=5000,
        )
        try:
            committed = admin.list_consumer_group_offsets(group_id)
        except Exception as exc:
            log.warning("KafkaAdminClient failed for group %s: %s", group_id, exc)
            return -1
        finally:
            admin.close()

        if not committed:
            return -1

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            request_timeout_ms=5000,
        )
        try:
            end_offsets = consumer.end_offsets(list(committed.keys()))
        except Exception as exc:
            log.warning("Failed to fetch end offsets: %s", exc)
            return -1
        finally:
            consumer.close()

        return sum(
            max(0, end_offsets.get(tp, om.offset) - om.offset)
            for tp, om in committed.items()
        )

    def container_running(self, container_name: str) -> Optional[str]:
        """Returns the container status string, or None if not found."""
        try:
            client = docker_sdk.from_env()
            return client.containers.get(container_name).status
        except docker_sdk.errors.NotFound:
            return None
        except Exception as exc:
            log.warning("Docker status check failed for %s: %s", container_name, exc)
            return None

    def start_container(self, container_name: str) -> None:
        """Start a stopped container (no-op if already running)."""
        client = docker_sdk.from_env()
        container = client.containers.get(container_name)
        if container.status != "running":
            container.start()
            log.info("Started container %s", container_name)
        else:
            log.info("Container %s already running", container_name)

    def stop_container(self, container_name: str, timeout: int = 30) -> None:
        """Send SIGTERM to a container and wait up to timeout seconds.

        timeout=30 gives the storage consumer time to flush its current batch
        before SIGKILL is sent. Producers have their own backoff logic and
        commit state is in Kafka, so 30s is conservative but safe.
        """
        client = docker_sdk.from_env()
        container = client.containers.get(container_name)
        if container.status == "running":
            container.stop(timeout=timeout)
            log.info("Stopped container %s", container_name)
        else:
            log.info("Container %s was not running (status: %s)", container_name, container.status)


class SparkClusterResource(ConfigurableResource):
    """Submits batch jobs via `docker exec` into the running spark-master container."""
    container_name: str = "spark-master"

    def submit(self, args: list[str]) -> None:
        import shlex
        bash_cmd = (
            "PYTHONPATH=/opt/project "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.executorEnv.PYTHONPATH=/opt/project "
            "--conf spark.executorEnv.MINIO_ENDPOINT=$MINIO_ENDPOINT "
            "--conf spark.executorEnv.MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY "
            "--conf spark.executorEnv.MINIO_SECRET_KEY=$MINIO_SECRET_KEY "
            f"/opt/project/main.py {shlex.join(args)}"
        )
        log.info("Submitting to %s: %s", self.container_name, bash_cmd)
        client    = docker_sdk.from_env()
        container = client.containers.get(self.container_name)
        exit_code, output = container.exec_run(["bash", "-c", bash_cmd], demux=False)
        if output:
            log.info(output.decode(errors="replace"))
        if exit_code != 0:
            raise RuntimeError(
                f"Spark job failed (exit {exit_code}):\n{output.decode(errors='replace') if output else ''}"
            )
