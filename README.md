# Installation et Configuration Apache Flink : Aperçu Général

Bienvenue dans la section Installation et Configuration d'Apache Flink de notre cours, conçue pour vous doter des compétences et connaissances nécessaires pour configurer Apache Flink efficacement. Cette section est essentielle pour quiconque souhaite travailler avec Apache Flink, car elle couvre les aspects fondamentaux de l'installation et de la configuration.

### Sujets Clés Couverts :

1 - Installation de Java : Nous commençons par vous guider dans l'installation de Java, qui est crucial pour exécuter Apache Flink.

2 - Installation d'Apache Flink : Des instructions étape par étape sont fournies sur la façon de télécharger, installer et configurer Apache Flink sur votre système.

3 - Gestion de Cluster : Apprenez à démarrer et arrêter votre cluster Flink, une compétence essentielle pour gérer vos applications Flink.

4 - Préparation de PyFlink : Nous couvrons les prérequis de PyFlink, y compris l'installation de Python et la configuration des outils nécessaires comme pip.

5 - Installation de PyFlink : Des étapes détaillées sont fournies pour installer PyFlink, vous préparant au développement dans cet environnement.

Cette section est vitale car elle jette les bases de votre parcours avec Apache Flink. À la fin de ces cours, vous disposerez d'un environnement Apache Flink entièrement opérationnel, ouvrant la voie à l'exploration des capacités et applications plus avancées de ce puissant framework de traitement.

# Etape 1: Intallation Java

Ouvrez vot écran et tape la commande suivantes:
```bash
sudo apt update

sudo apt install default-jre

sudo apt install default-jdk

javac --version
```
# Etape 2 Intallation d'apache flink
On télécharge le paquet en se redant sur le site officiel d'apache flink
```bash
wget https://dlcdn.apache.org/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz
# extraire le fichier
tar xzf flink-2.2.1-bin-scala_2.12.tgz
# on renome le fichier
mv flink-2.2.1 flink
sudo mv flink /opt/flink
```
Ensuite on demaare apache flink
```bash
# Pour démarrer
/opt/flink/bin/start-cluster.sh

# Pour arrêter
/opt/flink/bin/stop-cluster.sh
```
On y accede via le lien suivant: ```http://localhost:8081```

# Télécharger ficher 
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

# On copie le fichier 
```bash
scp -r '/home/farouk/Desktop/Flink/jars/flink-sql-connector-kafka-4.0.1-2.0.jar' root@app01.dev.datan.mgt.proxad.net:/home/hfarouk/tests/flink
flink-sql-connector-kafka-4.0.1-2.0.jar                             
```

# Etape 3 Installation pyFlink
On install pyFlink afin de developper sur flink avec python
```bash
python3 -m venv .venv

source .venv/bin/activate

pip install apache-flink==2.2.1

# parfois on a un soucie
export PATH="/home/farouk/Desktop/Flink/.venv/bin:$PATH"
which python3
# → doit afficher .venv/bin/python3
```


# cas de conflit java
```bash
update-alternatives --display java
# basculer vers java 21
sudo update-alternatives --set java /usr/lib/jvm/java-21-openjdk-amd64/bin/java
sudo update-alternatives --set javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac
```

ON arrete et redemarre
```bash
/opt/flink-server/bin/flink cancel 295bd9b65fd8f61292ecfafc33052f7c 2>/dev/null
/opt/flink-server/bin/stop-cluster.sh
/opt/flink-server/bin/start-cluster.sh
sleep 8
```
On lance avec le webmanager

```bash
cd /home/hfarouk/tests/flink
/opt/flink-server/bin/flink run --python job_freepro.py \
  --jarfile jars/flink-sql-connector-kafka-4.0.1-2.0.jar \
  -pyexec /home/hfarouk/tests/flink/.venv/bin/python \
  -pyclientexec /home/hfarouk/tests/flink/.venv/bin/python

```

# Environnement de dev local (Docker Compose)

Un `docker-compose.yml` fournit un environnement local complet pour tester les jobs
(`freepro`, `UPR`) sans toucher aux clusters Kafka de prod/preprod :

- **kafka** : un broker Kafka (KRaft, sans Zookeeper) exposé sur `localhost:9092`.
- **kafka-ui** : UI web sur `http://localhost:8080` pour inspecter topics/messages.
- **minio** : stockage S3-compatible pour les checkpoints Flink, UI sur
  `http://localhost:9001` (login `minioadmin` / `minioadmin`), API sur
  `http://localhost:9000`. Le bucket `flink-checkpoints` est cree
  automatiquement par le service `minio-init` (one-shot, via `mc mb`).
- **jobmanager** / **taskmanager** : cluster Flink 2.2.1 (build local via
  `flink-jobs/Dockerfile`, Flink + Python3 + `apache-flink==2.2.1`), UI sur
  `http://localhost:8081`. Checkpoints stockes sur MinIO, un sous-dossier par
  job (`s3://flink-checkpoints/freepro/checkpoints`,
  `s3://flink-checkpoints/UPR/checkpoints`, plugin `flink-s3-fs-hadoop`).

`flink-jobs/` est monté dans les conteneurs Flink et pointe par defaut vers les
brokers de prod ; en local, `SRC_BROKERS`/`DST_BROKERS` sont surchargés vers
`kafka:19092` (meme broker pour source et sink, simplement des topics
differents). Le bucket de checkpoints est surchargeable via
`CHECKPOINTS_BUCKET` (defaut `flink-checkpoints`).

Structure de `flink-jobs/` :
- un sous-dossier par job (`freepro/`, `UPR/`), avec son `job_*.py` ;
- `common/` : config/env partages entre jobs (env variables, table
  environment/checkpointing) - voir `flink-jobs/common/`;
- `_template/` : squelette a copier pour demarrer un nouveau job ;
- `jars/` : jar du connecteur Kafka, partage par tous les jobs.

### Variables d'environnement

| Variable | Service(s) | Defaut (code) | Defaut en local (docker-compose.yml) | Role |
| --- | --- | --- | --- | --- |
| `SRC_BROKERS` | `freepro`, `UPR` (`flink-jobs/common/settings.py`) | brokers Kafka prod (`kafka01/02/03.bip.mgt.proxad.net:9092`) | `kafka:19092` | Brokers Kafka source (lecture des topics `telemetry_events*`). |
| `DST_BROKERS` | `freepro`, `UPR` (`flink-jobs/common/settings.py`) | brokers Kafka preprod (`app-ppr01/02.dev.datan.mgt.proxad.net:9092`) | `kafka:19092` | Brokers Kafka sink (ecriture des topics de sortie). |
| `CHECKPOINTS_BUCKET` | `freepro`, `UPR` (`flink-jobs/common/settings.py`) | `flink-checkpoints` | `flink-checkpoints` (non surcharge) | Bucket S3/MinIO ou sont stockes les checkpoints Flink, sous `s3://<bucket>/<job_name>/checkpoints`. Doit correspondre au bucket cree par `minio-init`. |
| `BROKERS` | `mock-producer` | `kafka:19092` | `kafka:19092` | Brokers Kafka vers lesquels le mock-producer publie. |
| `TOPIC_METADATA` | `mock-producer` | `telemetry_events_metadata` | (non surcharge) | Topic de sortie pour les events metadata (vendor-info SFP + profil). |
| `TOPIC_EVENTS` | `mock-producer` | `telemetry_events` | (non surcharge) | Topic de sortie pour les events discrets (unexpected-onu, mpcp-register-acks-timeouts, statut up/down). |
| `TOPIC_METRICS` | `mock-producer` | `telemetry_metrics` | (non surcharge) | Topic de sortie pour les metriques continues (sfp mon tx/rx/temp, compteurs de trames). |
| `DEVICE_COUNT` | `mock-producer` | `10` | (non surcharge) | Taille du pool de devices mock generes. |
| `SLEEP_SECONDS` | `mock-producer` | `3` | (non surcharge) | Pause entre deux boucles de generation (tous les devices publient a chaque boucle). |

Toutes surchargeables au besoin dans `docker-compose.yml` (section `environment:`
du service concerne) ou via `docker compose run -e VAR=valeur ...`.

### Demarrer

```bash
docker compose up -d --build
```

### Creer les topics

Necessaire : Flink liste les topics via AdminClient, qui ne declenche pas
l'auto-creation meme si `auto.create.topics.enable=true` (et ce flag est de
toute facon desactive sur le broker de dev).

```bash
./scripts/create-topics.sh
```

Cree les topics utilises par les jobs (`telemetry_events_metadata`,
`telemetry_events`, `telemetry_metrics`, `events_metadata_freepro`,
`virtports_events_freepro`, `virtports_metrics_freepro`, `virtports_events_upr`),
1 partition / 1 replica chacun.
Le script est idempotent (`--if-not-exists`), on peut le relancer sans risque.

### Generer des mock events en continu

Le service `mock-producer` (voir `mock-producer/`) tourne en boucle tant que le
conteneur n'est pas arrete, et publie des events au format telemetry
(`name`/`timestamp`/`tags`/`values`) sur un pool de devices mock :

- `telemetry_events_metadata` : vendor-info SFP + le path
  `/epon-virtport/virtport/profile` (moitie du pool en profil `6`, moitie en
  autre profil, pour verifier que `job_freepro.py` filtre bien).
- `telemetry_events` : events `unexpected-onu` / `mpcp-register-acks-timeouts`.
- `telemetry_metrics` : metriques SFP (tx/rx/temp) et compteurs de trames ; les
  metriques portant un `virtport_virtport-id` sont enrichies par
  `job_freepro.py` (profil 6 uniquement) et republiees sur
  `virtports_metrics_freepro`.

Demarre automatiquement avec `docker compose up -d --build`. Pour le
piloter independamment :

```bash
docker compose up -d --build mock-producer   # (re)demarrer
docker compose logs -f mock-producer         # suivre la production
docker compose stop mock-producer            # arreter
```

Reglable via les variables d'environnement du service, voir la section
[Variables d'environnement](#variables-denvironnement).

### Soumettre les jobs

`flink-jobs/` contient un sous-dossier par job ; ils tournent tous sur le meme
cluster jobmanager/taskmanager (soumission independante, plusieurs jobs actifs
en meme temps) :

```bash
docker compose exec jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/usrlib/freepro/job_freepro.py \
  --jarfile /opt/flink/usrlib/jars/flink-sql-connector-kafka-4.0.1-2.0.jar \
  -pyexec /usr/bin/python3 \
  -pyclientexec /usr/bin/python3 -d

docker compose exec jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/usrlib/UPR/job_upr.py \
  --jarfile /opt/flink/usrlib/jars/flink-sql-connector-kafka-4.0.1-2.0.jar \
  -pyexec /usr/bin/python3 \
  -pyclientexec /usr/bin/python3 -d
```

`UPR` reprend la logique de l'ancien script `streaming_telemetry.py` (portage
Flink) : ne garde que les lignes de `telemetry_events` avec un
`virtport_virtport-id` et un `path` contenant `up`, publie le resultat sur
`virtports_events_upr`. Contrairement a `freepro`, il lit en `latest-offset` (pas
d'historique) et ecrit en append simple (pas d'upsert-kafka).

Suivre l'execution sur `http://localhost:8081`.

### Injecter des messages de test

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic telemetry_events_metadata <<'EOF'
{"name":"m1","timestamp":1,"tags":{"virtport_virtport-id":"vp1","path":"/epon-virtport/virtport/profile","nra_nro":"nra1","source":"srcA"},"values":{"/epon-virtport/virtport/profile":"6"}}
EOF

docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic telemetry_events <<'EOF'
{"name":"e1","timestamp":100,"tags":{"virtport_virtport-id":"vp1","path":"/event/foo"},"values":{"/event/foo":"bar"}}
EOF
```

### Lire les topics de sortie

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic events_metadata_freepro --from-beginning

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic virtports_events_freepro --from-beginning

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic virtports_metrics_freepro --from-beginning

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic virtports_events_upr --from-beginning
```

### Arreter et nettoyer

```bash
docker compose down -v
```
