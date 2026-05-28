import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from dagster import ConfigurableResource
from minio import Minio


@dataclass
class PodHealth:
    phase: str          # "running", "pending", "failed", "unknown"
    ready: bool         # container ready flag from k8s
    restart_count: int  # cumulative container restarts

log = logging.getLogger(__name__)

_PIPELINE_NS    = "mekong-pipeline"
_PROCESSING_NS  = "mekong-processing"


def _k8s():
    """Return kubernetes client module with config loaded."""
    from kubernetes import client, config as k8s_config
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return client


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
        import pyarrow as pa
        import pyarrow.dataset as ds
        import s3fs

        fs = s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            endpoint_url=self.endpoint,
            use_ssl=self.endpoint.startswith("https://"),
        )
        # Explicit partition schema prevents PyArrow from treating Spark's zero-byte
        # directory-marker objects (e.g. "month=05/") as parquet files, which causes
        # FileNotFoundError. exclude_invalid_files skips any remaining non-parquet objects.
        partitioning = ds.partitioning(
            pa.schema([
                ("asset_class", pa.string()),
                ("year",        pa.string()),
                ("month",       pa.string()),
                ("day",         pa.string()),
            ]),
            flavor="hive",
        )
        dataset = ds.dataset(
            f"{bucket}/{dataset_path}",
            filesystem=fs,
            format="parquet",
            partitioning=partitioning,
            exclude_invalid_files=True,
        )
        return dataset.to_table(filter=filter_expr)


class KafkaAdminResource(ConfigurableResource):
    bootstrap_servers: str

    def topic_end_offset(self, topic: str) -> int:
        """Sum of end offsets across all partitions for topic. Returns -1 on error."""
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            request_timeout_ms=5000,
        )
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return -1
            tps = [TopicPartition(topic, p) for p in partitions]
            return sum(consumer.end_offsets(tps).values())
        except Exception as exc:
            log.warning("Failed to get end offset for %s: %s", topic, exc)
            return -1
        finally:
            consumer.close()

    def consumer_group_lag(self, group_id: str) -> int:
        """Total uncommitted messages for group_id. Returns -1 on error or no offsets."""
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

    def pod_health(self, app_label: str) -> Optional[PodHealth]:
        """Returns PodHealth for the newest pod with app={app_label}, or None if not found."""
        try:
            k8s = _k8s()
            pods = k8s.CoreV1Api().list_namespaced_pod(
                namespace=_PIPELINE_NS,
                label_selector=f"app={app_label}",
            )
            if not pods.items:
                return None
            pod = max(pods.items, key=lambda p: p.metadata.creation_timestamp)
            phase = (pod.status.phase or "unknown").lower()
            ready = False
            restart_count = 0
            if pod.status.container_statuses:
                cs = pod.status.container_statuses[0]
                ready = bool(cs.ready)
                restart_count = int(cs.restart_count or 0)
            return PodHealth(phase=phase, ready=ready, restart_count=restart_count)
        except Exception as exc:
            log.warning("K8s pod check failed for %s: %s", app_label, exc)
            return None

    def start_container(self, app_label: str) -> None:
        """Scale deployment to 1 replica."""
        try:
            _k8s().AppsV1Api().patch_namespaced_deployment_scale(
                name=app_label,
                namespace=_PIPELINE_NS,
                body={"spec": {"replicas": 1}},
            )
            log.info("Scaled %s/%s to 1 replica", _PIPELINE_NS, app_label)
        except Exception as exc:
            raise RuntimeError(f"Failed to start {app_label}: {exc}") from exc

    def stop_container(self, app_label: str, timeout: int = 30) -> None:
        """Scale deployment to 0 replicas."""
        try:
            _k8s().AppsV1Api().patch_namespaced_deployment_scale(
                name=app_label,
                namespace=_PIPELINE_NS,
                body={"spec": {"replicas": 0}},
            )
            log.info("Scaled %s/%s to 0 replicas", _PIPELINE_NS, app_label)
        except Exception as exc:
            raise RuntimeError(f"Failed to stop {app_label}: {exc}") from exc


class SparkClusterResource(ConfigurableResource):
    spark_image: str = "ghcr.io/davidhoang2406/mekong-spark:latest"
    namespace: str = "mekong-processing"
    service_account: str = "spark"

    def submit(self, args: list[str]) -> None:
        """Create a SparkApplication CRD and block until COMPLETED or FAILED."""
        k8s = _k8s()
        custom_api = k8s.CustomObjectsApi()

        job_name = f"spark-{args[0]}-{int(time.time())}"
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio.mekong-data.svc.cluster.local:9000")

        _env = [
            {"name": "MINIO_ENDPOINT",   "value": minio_endpoint},
            {"name": "PYTHONPATH",        "value": "/opt/project"},
            {"name": "MINIO_ACCESS_KEY",  "valueFrom": {"secretKeyRef": {"name": "minio-credentials", "key": "access-key"}}},
            {"name": "MINIO_SECRET_KEY",  "valueFrom": {"secretKeyRef": {"name": "minio-credentials", "key": "secret-key"}}},
        ]

        spark_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": job_name, "namespace": self.namespace},
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": self.spark_image,
                "imagePullPolicy": "Always",
                "mainApplicationFile": "local:///opt/project/main.py",
                "arguments": args,
                "sparkVersion": "4.1.1",
                "driver": {
                    "cores": 1,
                    "memory": "1g",
                    "serviceAccount": self.service_account,
                    "env": _env,
                },
                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "1g",
                    "env": _env,
                },
            },
        }

        log.info("Creating SparkApplication %s args=%s", job_name, args)
        custom_api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            body=spark_app,
        )

        deadline = time.monotonic() + 3600
        _TERMINAL = {"COMPLETED", "FAILED", "SUBMISSION_FAILED", "INVALIDATING"}

        while time.monotonic() < deadline:
            time.sleep(30)
            try:
                obj = custom_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    name=job_name,
                )
                state = obj.get("status", {}).get("applicationState", {}).get("state") or "UNKNOWN"
                log.info("SparkApplication %s → %s", job_name, state)
                if state == "COMPLETED":
                    return
                if state in _TERMINAL:
                    err = obj.get("status", {}).get("applicationState", {}).get("errorMessage", "")
                    raise RuntimeError(f"Spark job {job_name} failed ({state}): {err}")
            except RuntimeError:
                raise
            except Exception as exc:
                log.warning("Poll error for %s: %s", job_name, exc)

        raise RuntimeError(f"Spark job {job_name} timed out after 3600s")
