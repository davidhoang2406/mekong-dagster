import logging

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
