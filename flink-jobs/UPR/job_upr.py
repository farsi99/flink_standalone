"""
Reecriture Flink de l'ancien script streaming_telemetry.py (consommateur/producteur Kafka
"a la main", pas de Flink).

Le format des messages telemetry_events est un objet JSON par message
(`name`/`timestamp`/`tags`/`values`), pas un tableau d'entrees comme le
supposait l'ancien script - meme format que celui deja lu par
flink-jobs/freepro/job_freepro.py et produit par mock-producer.

Logique inchangee : ne garder que les lignes qui portent un
virtport_virtport-id, et dont le path (cle de `values`) contient "up".
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_DIR))

from common.env import create_table_env
from common.settings import SRC_BROKERS, DST_BROKERS

# --- CONFIG : topics / group id ---

SRC_TOPIC = "telemetry_events"
SINK_TOPIC = "virtports_events_upr"
GROUP_ID = "telemetry_group"

table_env = create_table_env(job_name="UPR")

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

table_env.execute_sql("DROP TABLE IF EXISTS sink_virtports_events_upr")
table_env.execute_sql(f"""
CREATE TABLE sink_virtports_events_upr (
    `timestamp` BIGINT,
    tags        MAP<STRING, STRING>,
    path        STRING,
    `value`     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{SINK_TOPIC}',
    'properties.bootstrap.servers' = '{DST_BROKERS}',
    'format' = 'json'
)
""")

# --- LOGIQUE : ne garder que virtport_virtport-id present + path contenant "up" ---
table_env.execute_sql(f"""
INSERT INTO sink_virtports_events_upr
SELECT
    `timestamp`,
    tags,
    tags['path']           AS path,
    `values`[tags['path']] AS `value`
FROM {SRC_TOPIC}
WHERE tags['virtport_virtport-id'] IS NOT NULL
  AND tags['path'] LIKE '%up%'
""").wait()

print(f"JOB UPR : {SRC_TOPIC} -> {SINK_TOPIC} (path contient 'up') ... Ctrl+C pour arreter")
