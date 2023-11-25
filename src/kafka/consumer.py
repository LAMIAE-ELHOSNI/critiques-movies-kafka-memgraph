import requests
from confluent_kafka import Consumer, KafkaException
import json
import logging

# Configurer les logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration du consommateur Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du broker Kafka
    'group.id': 'my_group',  # ID du groupe de consommateurs
    'auto.offset.reset': 'earliest'  # Commencer à consommer depuis le début du topic
}

# Créer une instance du consommateur Kafka
consumer = Consumer(consumer_config)

# S'abonner au topic Kafka
topic = "critiques-movie"  # Remplacez avec votre topic Kafka
consumer.subscribe([topic])

# Configuration de l'URL de Memgraph
url = "http://localhost:7687"

try:
    logger.info(f"Kafka Consumer Configuration: {consumer_config}")
    while True:
        msg = consumer.poll(5.0)  # Consommer les messages, attendre jusqu'à 5 secondes pour de nouveaux messages

        if msg is None:
            logger.info("Aucun nouveau message reçu. Sortie.")
            break
        elif not msg.error():
            # Loguer les messages reçus depuis Kafka
            logger.info(f"Message reçu depuis Kafka : {msg.value().decode('utf-8')}")

            # Faire quelque chose avec les données reçues (par exemple, les insérer dans Memgraph)
            data = json.loads(msg.value().decode('utf-8'))

            # Adapter les données au format d'insertion dans Memgraph
            transformed_data = {
                "userId": data.get("userId", ""),
                "movieId": data.get("movie", {}).get("movieId", ""),
                "title": data.get("movie", {}).get("title", ""),
                "genres": data.get("movie", {}).get("genres", []),
                "rating": data.get("rating", ""),
                "timestamp": data.get("timestamp", "")
            }

            query = """
                WITH $data AS data
                MERGE (user:User {userId: data.userId})
                MERGE (movie:Movie {movieId: data.movie.movieId})
                ON CREATE SET movie.title = data.movie.title

                FOREACH (genre IN data.movie.genres |
                    MERGE (g:Genre {name: genre})
                    MERGE (movie)-[:IS_GENRE]->(g)
                )
            
               MERGE (user)-[r:RATED]->(movie)
               SET r.rating = toInteger(data.rating), r.timestamp = toInteger(data.timestamp)
            """


            response = requests.post(url=url, json={"query": query, "params": {"data": transformed_data}})
            if response.status_code == 200:
                logger.info(f"Données insérées avec succès dans Memgraph : {transformed_data}")
            else:
                logger.error(f"Erreur lors de l'insertion dans Memgraph : {response.text}")

        else:
            logger.error(f"Erreur lors de la consommation de messages : {msg.error()}")

except KeyboardInterrupt:
    logger.info("Arrêt du consommateur Kafka")
except KafkaException as kafka_exception:
    logger.error(f"Erreur de connexion à Kafka : {kafka_exception}")
except Exception as e:
    logger.error(f"Une erreur inattendue est survenue : {e}")
finally:
    consumer.close()
