"""
JOB UNIQUE - Filtre profil 6 et met a disposition les 2 flux enrichis.

Remplace l'ancien duo Job1 + Job2 (relies par un topic upsert-kafka de reference,
qui generait du churn / des tombstones et vidait virtports_events_freepro).

Ici la whitelist profil 6 est une VUE calculee dans le meme job depuis les metadata
(la ligne 'path=.../profile' porte deja nra_nro + source). Les deux sinks sont donc
alimentes par des joins INSERT-ONLY (whitelist append-only x flux append) : aucun
tombstone, rien n'est retracte/supprime en aval.

- events_metadata_freepro : metadata des virtports profil 6.
- virtports_events_freepro : events des virtports profil 6, enrichis nra_nro/source.
"""
from pyflink.table import EnvironmentSettings, TableEnvironment
import os

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

cfg = table_env.get_config().get_configuration()
cfg.set_string("execution.checkpointing.interval", "30 s")
cfg.set_string("execution.checkpointing.mode", "AT_LEAST_ONCE")
cfg.set_string("table.exec.source.idle-timeout", "30 s")
    
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JAR_PATH = f"file://{os.path.join(CURRENT_DIR, 'jars/flink-sql-connector-kafka-4.0.1-2.0.jar')}"
cfg.set_string("pipeline.jars", JAR_PATH)

SRC_BROKERS = "kafka01.bip.mgt.proxad.net:9092,kafka02.bip.mgt.proxad.net:9092,kafka03.bip.mgt.proxad.net:9092"
DST_BROKERS = "app-ppr01.dev.datan.mgt.proxad.net:9092,app-ppr02.dev.datan.mgt.proxad.net:9092"

for t in ("telemetry_events_metadata", "telemetry_events",
          "sink_events_metadata", "sink_virtports_events"):
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

print("JOB FREEPRO : filtre profil 6 -> events_metadata_freepro + virtports_events_freepro ... Ctrl+C pour arreter")
table_env_result = statement_set.execute()
table_env_result.wait()
