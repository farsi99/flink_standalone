"""
Table environment partage par tous les jobs : checkpointing (30s, S3 via
MinIO en local), jar du connecteur Kafka. Chaque job passe juste son nom,
qui sert de sous-dossier de checkpoints
(s3://flink-checkpoints/{job_name}/checkpoints).
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

from common.settings import CHECKPOINTS_BUCKET

_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
_FLINK_JOBS_DIR = os.path.dirname(_COMMON_DIR)
JAR_PATH = f"file://{os.path.join(_FLINK_JOBS_DIR, 'jars', 'flink-sql-connector-kafka-4.0.1-2.0.jar')}"


def create_table_env(job_name):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    cfg = table_env.get_config().get_configuration()
    cfg.set_string("execution.checkpointing.interval", "30 s")
    cfg.set_string("execution.checkpointing.mode", "AT_LEAST_ONCE")
    cfg.set_string("table.exec.source.idle-timeout", "30 s")
    cfg.set_string("execution.checkpointing.storage", "filesystem")
    cfg.set_string("state.checkpoints.dir", f"s3://{CHECKPOINTS_BUCKET}/{job_name}/checkpoints")
    cfg.set_string("pipeline.jars", JAR_PATH)
    cfg.set_string("pipeline.name", job_name)

    return table_env
