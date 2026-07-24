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
