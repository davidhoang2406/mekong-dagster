from pydantic import Field

from dagster import Config, OpExecutionContext, job, op

from dagster_project.resources import KafkaAdminResource

_ALL_DAEMONS = ["storage-consumer", "stock-price-producer", "crypto-price-producer"]

# Canonical order matters:
#   start → consumer first so it's ready before messages arrive
#   stop  → producers first so no new messages land while consumer is draining
_START_ORDER = ["storage-consumer", "stock-price-producer", "crypto-price-producer"]
_STOP_ORDER  = ["stock-price-producer", "crypto-price-producer", "storage-consumer"]


class KafkaDaemonConfig(Config):
    containers: list[str] = Field(
        default_factory=lambda: list(_ALL_DAEMONS),
        description=(
            "Which containers to operate on. Defaults to all three daemons. "
            f"Valid values: {_ALL_DAEMONS}"
        ),
    )


@op(required_resource_keys={"kafka_admin"})
def start_kafka_daemons(context: OpExecutionContext, config: KafkaDaemonConfig) -> None:
    kafka: KafkaAdminResource = context.resources.kafka_admin
    unknown = set(config.containers) - set(_ALL_DAEMONS)
    if unknown:
        raise ValueError(f"Unknown container name(s): {sorted(unknown)}. Valid: {_ALL_DAEMONS}")
    # Preserve canonical start order for whatever subset was requested
    selected = [n for n in _START_ORDER if n in config.containers]
    for name in selected:
        health = kafka.pod_health(name)
        if health is None:
            raise RuntimeError(
                f"Pod '{name}' not found — check that the deployment exists in mekong-pipeline."
            )
        if health.phase == "running":
            context.log.info("%s is already running — skipping", name)
        else:
            kafka.start_container(name)
            context.log.info("Started %s (was: %s)", name, health.phase)


@op(required_resource_keys={"kafka_admin"})
def stop_kafka_daemons(context: OpExecutionContext, config: KafkaDaemonConfig) -> None:
    kafka: KafkaAdminResource = context.resources.kafka_admin
    unknown = set(config.containers) - set(_ALL_DAEMONS)
    if unknown:
        raise ValueError(f"Unknown container name(s): {sorted(unknown)}. Valid: {_ALL_DAEMONS}")
    # Preserve canonical stop order for whatever subset was requested
    selected = [n for n in _STOP_ORDER if n in config.containers]
    for name in selected:
        health = kafka.pod_health(name)
        if health is None:
            context.log.warning("%s not found — nothing to stop", name)
            continue
        if health.phase != "running":
            context.log.info("%s is not running (phase: %s) — skipping", name, health.phase)
        else:
            kafka.stop_container(name)
            context.log.info("Stopped %s", name)


@job(description="Start Kafka pipeline daemons. Set `containers` in config to start a subset.")
def start_kafka_pipeline_job():
    start_kafka_daemons()


@job(description="Stop Kafka pipeline daemons. Set `containers` in config to stop a subset.")
def stop_kafka_pipeline_job():
    stop_kafka_daemons()
