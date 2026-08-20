#!/usr/bin/env bash
# Cree les topics Kafka utilises par les jobs (freepro, UPR) dans l'env de dev local.
# Necessaire car Flink liste les topics via AdminClient, qui ne declenche pas
# l'auto-creation meme si auto.create.topics.enable=true (et ce flag est
# desactive sur le broker de dev).
set -euo pipefail

TOPICS=(
  telemetry_events_metadata
  telemetry_events
  telemetry_metrics
  events_metadata_freepro
  virtports_events_freepro
  virtports_metrics_freepro
  virtports_events_upr
)

for t in "${TOPICS[@]}"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done
