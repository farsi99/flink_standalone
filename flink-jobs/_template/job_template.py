"""
Squelette de job Flink : copier ce dossier pour demarrer un nouveau job.

Seule la partie "LOGIQUE" en bas doit changer d'un job a l'autre ; l'env
(checkpointing/S3/jar) et le broker Kafka sont mutualises dans
flink-jobs/common/.
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_DIR))

from common.env import create_table_env
from common.settings import SRC_BROKERS, DST_BROKERS

# --- CONFIG : a adapter pour le nouveau job ---

JOB_NAME = "template"  # -> s3://flink-checkpoints/{JOB_NAME}/checkpoints
SRC_TOPIC = "telemetry_events"
SINK_TOPIC = "my_sink_topic"
GROUP_ID = "my_group_id"

table_env = create_table_env(job_name=JOB_NAME)

table_env.execute_sql(f"DROP TABLE IF EXISTS {SRC_TOPIC}")
table_env.execute_sql(f"""
CREATE TABLE {SRC_TOPIC} (
    name        STRING,
    `timestamp` BIGINT,
    tags        MAP<STRING, STRING>,
    `values`    MAP<STRING, STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = '{SRC_TOPIC}',
    'properties.bootstrap.servers' = '{SRC_BROKERS}',
    'properties.group.id' = '{GROUP_ID}',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
""")

table_env.execute_sql("DROP TABLE IF EXISTS my_sink")
table_env.execute_sql(f"""
CREATE TABLE my_sink (
    `timestamp` BIGINT,
    `value`     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{SINK_TOPIC}',
    'properties.bootstrap.servers' = '{DST_BROKERS}',
    'format' = 'json'
)
""")

# --- LOGIQUE : a ecrire ---
table_env.execute_sql(f"""
INSERT INTO my_sink
SELECT `timestamp`, `values`[tags['path']] AS `value`
FROM {SRC_TOPIC}
WHERE tags['path'] IS NOT NULL
""").wait()

print(f"JOB {JOB_NAME} : {SRC_TOPIC} -> {SINK_TOPIC} ... Ctrl+C pour arreter")
