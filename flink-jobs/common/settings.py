"""
Brokers Kafka, avec les defauts de prod/preprod. Surcharge via les variables
d'environnement SRC_BROKERS / DST_BROKERS (c'est ce que fait docker-compose.yml
en local, pour pointer vers le broker Kafka du dev-env au lieu de la prod).
"""
import os

SRC_BROKERS = os.environ.get(
    "SRC_BROKERS",
    "kafka01.bip.mgt.proxad.net:9092,kafka02.bip.mgt.proxad.net:9092,kafka03.bip.mgt.proxad.net:9092",
)
DST_BROKERS = os.environ.get(
    "DST_BROKERS",
    "app-ppr01.dev.datan.mgt.proxad.net:9092,app-ppr02.dev.datan.mgt.proxad.net:9092",
)

CHECKPOINTS_BUCKET = os.environ.get("CHECKPOINTS_BUCKET", "flink-checkpoints")
