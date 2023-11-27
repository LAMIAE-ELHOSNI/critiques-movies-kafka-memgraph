<<<<<<< HEAD
# critiques-movies-kafka-memgraph
=======
# Set Up Environnement
creer un reseau docker
```
sudo docker network create kafka_memgraph_network
```
lancer le service kafka
```
sudo docker run -d --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 --network kafka_memgraph_network zookeeper:latest
sudo docker run -d --name kafka -p 9092:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    --network kafka_memgraph_network confluentinc/cp-kafka:latest
```
Lancer le service Memgraph Lab
```
sudo docker pull memgraph/memgraph-platform:latest
sudo docker tag memgraph/memgraph-platform:latest memgraph
sudo docker run -d -p 7687:7687 -p 3000:3000 --name memgraph --network kafka_memgraph_network memgraph --bolt-server-name-for-init=Neo4j/5.2.0

```

ou executer la commande suivante:
```
sudo docker-compose up -d

```

>>>>>>> 978512e (First Commit)
