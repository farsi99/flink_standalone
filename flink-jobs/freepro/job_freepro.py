"""
JOB UNIQUE - Filtre profil 6 et met a disposition les 3 flux enrichis.

Remplace l'ancien duo Job1 + Job2 (relies par un topic upsert-kafka de reference,
qui generait du churn / des tombstones et vidait virtports_events_freepro).

Ici la whitelist profil 6 est une VUE calculee dans le meme job depuis les metadata
(la ligne 'path=.../profile' porte deja nra_nro + source). Les trois sinks sont donc
alimentes par des joins INSERT-ONLY (whitelist append-only x flux append) : aucun
tombstone, rien n'est retracte/supprime en aval.

- events_metadata_freepro : metadata des virtports profil 6.
- virtports_events_freepro : events des virtports profil 6, enrichis nra_nro/source.
- virtports_metrics_freepro : metriques (telemetry_metrics) des virtports profil 6, enrichis nra_nro/source.
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_DIR))

from common.env import create_table_env
from common.settings import DST_BROKERS, SRC_BROKERS

table_env = create_table_env(job_name="freepro")

for t in ("telemetry_events_metadata", "telemetry_events", "telemetry_metrics",
          "sink_events_metadata", "sink_virtports_events", "sink_virtports_metrics"):
    table_env.execute_sql(f"DROP TABLE IF EXISTS {t}")
table_env.execute_sql("DROP VIEW IF EXISTS v_virtports_profile6")

# --- SOURCE metadata (earliest : amorce toute la whitelist profil 6) ---
table_env.execute_sql(f"""
CREATE TABLE telemetry_events_metadata (
    name        STRING,
    `timestamp` BIGINT,
    tags        MAP<STRING, STRING>,
    `values`    MAP<STRING, STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry_events_metadata',
    'properties.bootstrap.servers' = '{SRC_BROKERS}',
    'properties.group.id' = 'flink-freepro-meta',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# --- SOURCE events (latest en nominal ; passer en earliest pour rejouer l'historique) ---
table_env.execute_sql(f"""
CREATE TABLE telemetry_events (
    name        STRING,
    `timestamp` BIGINT,
    tags        MAP<STRING, STRING>,
    `values`    MAP<STRING, STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry_events',
    'properties.bootstrap.servers' = '{SRC_BROKERS}',
    'properties.group.id' = 'flink-freepro-events',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# --- SOURCE metrics (earliest ; memes tags path/vlan_switch/nra_nro/source que les autres flux) ---
table_env.execute_sql(f"""
CREATE TABLE telemetry_metrics (
    name        STRING,
    `timestamp` BIGINT,
    tags        MAP<STRING, STRING>,
    `values`    MAP<STRING, STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry_metrics',
    'properties.bootstrap.servers' = '{SRC_BROKERS}',
    'properties.group.id' = 'flink-freepro-metrics',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# --- WHITELIST profil 6 (+ identite) : vue in-job, append-only ---
# La ligne 'path=.../profile' porte deja nra_nro + source. DISTINCT => un tuple stable
# par virtport, jamais retracte. On exige nra_nro/source non-null pour ne jamais
# enrichir avec du null.
table_env.execute_sql("""
CREATE VIEW v_virtports_profile6 AS
SELECT DISTINCT
    tags['virtport_virtport-id'] AS virtport_id,
    tags['nra_nro']              AS nra_nro,
    tags['source']               AS source
FROM telemetry_events_metadata
WHERE tags['path'] = '/epon-virtport/virtport/profile'
  AND `values`['/epon-virtport/virtport/profile'] = '6'
  AND tags['virtport_virtport-id'] IS NOT NULL
  AND tags['nra_nro'] IS NOT NULL
  AND tags['source'] IS NOT NULL
""")

# --- SINKS (upsert-kafka, etat courant par (virtport_id, path)) ---
table_env.execute_sql(f"""
CREATE TABLE sink_events_metadata (
    virtport_id   STRING,
    path          STRING,
    nra_nro       STRING,
    source        STRING,
    `value`       STRING,
    event_time_ns BIGINT,
    PRIMARY KEY (virtport_id, path) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'events_metadata_freepro',
    'properties.bootstrap.servers' = '{DST_BROKERS}',
    'key.format' = 'json',
    'value.format' = 'json'
)
""")

table_env.execute_sql(f"""
CREATE TABLE sink_virtports_events (
    virtport_id   STRING,
    path          STRING,
    nra_nro       STRING,
    source        STRING,
    `value`       STRING,
    event_time_ns BIGINT,
    PRIMARY KEY (virtport_id, path) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'virtports_events_freepro',
    'properties.bootstrap.servers' = '{DST_BROKERS}',
    'key.format' = 'json',
    'value.format' = 'json'
)
""")

table_env.execute_sql(f"""
CREATE TABLE sink_virtports_metrics (
    event_time_ns BIGINT,
    nra_nro       STRING,
    source        STRING,
    virtport_id   STRING,
    vlan_switch   STRING,
    path          STRING,
    `value`       STRING,
    PRIMARY KEY (virtport_id, path) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'virtports_metrics_freepro',
    'properties.bootstrap.servers' = '{DST_BROKERS}',
    'key.format' = 'json',
    'value.format' = 'json'
)
""")

statement_set = table_env.create_statement_set()

# Flux A : metadata des virtports profil 6 (une metrique = une ligne).
statement_set.add_insert_sql("""
INSERT INTO sink_events_metadata
SELECT /*+ STATE_TTL('m' = '30min', 'p' = '365d') */
    m.tags['virtport_virtport-id']  AS virtport_id,
    m.tags['path']                  AS path,
    p.nra_nro                       AS nra_nro,
    p.source                        AS source,
    m.`values`[m.tags['path']]      AS `value`,
    m.`timestamp`                   AS event_time_ns
FROM telemetry_events_metadata m
JOIN v_virtports_profile6 p
  ON m.tags['virtport_virtport-id'] = p.virtport_id
WHERE m.tags['path'] IS NOT NULL
""")

# Flux B : events des virtports profil 6, enrichis nra_nro/source depuis la whitelist.
statement_set.add_insert_sql("""
INSERT INTO sink_virtports_events
SELECT
    e.tags['virtport_virtport-id']  AS virtport_id,
    e.tags['path']                  AS path,
    p.nra_nro                       AS nra_nro,
    p.source                        AS source,
    e.`values`[e.tags['path']]      AS `value`,
    e.`timestamp`                   AS event_time_ns
FROM telemetry_events e
JOIN v_virtports_profile6 p
  ON e.tags['virtport_virtport-id'] = p.virtport_id
WHERE e.tags['path'] IS NOT NULL
""")

# Flux C : metriques (telemetry_metrics) des virtports profil 6, enrichis nra_nro/source depuis la whitelist.
statement_set.add_insert_sql("""
INSERT INTO sink_virtports_metrics
SELECT
    m.`timestamp`                   AS event_time_ns,
    p.nra_nro                       AS nra_nro,
    p.source                        AS source,
    m.tags['virtport_virtport-id']  AS virtport_id,
    m.tags['vlan_switch']           AS vlan_switch,
    m.tags['path']                  AS path,
    m.`values`[m.tags['path']]      AS `value`
FROM telemetry_metrics m
JOIN v_virtports_profile6 p
  ON m.tags['virtport_virtport-id'] = p.virtport_id
WHERE m.tags['path'] IS NOT NULL
""")

print("JOB FREEPRO : filtre profil 6 -> events_metadata_freepro + virtports_events_freepro + virtports_metrics_freepro ... Ctrl+C pour arreter")
table_env_result = statement_set.execute()
table_env_result.wait()
