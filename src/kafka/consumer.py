from confluent_kafka import Consumer
import json
import logging
from py2neo import Graph

# Configurer les logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Établir la connexion à Memgraph
uri = "bolt://localhost:7687"
username = "Fidelis"
password = "admin"
graph = None

try:
    graph = Graph(uri, auth=(username, password))
    logger.info("Connexion à Memgraph établie !")

    def transformer_donnees(message):
        try:
            # Convertir le message JSON en dictionnaire Python
            data = json.loads(message)

            # Adapter la structure des données au format attendu par Memgraph
            transformed_data = {
                "userId": data.get("userId", ""),
                "movie": {
                    "movieId": data.get("movieId", ""),
                    "title": data.get("title", ""),
                    "genres": data.get("genres", []),
                },
                "rating": data.get("rating", ""),
                "timestamp": data.get("timestamp", "")
            }

            return transformed_data

        except Exception as e:
            logger.error(f"Erreur lors de la transformation des données : {e}")
            return None

    def inserer_donnees_memgraph(data):
        try:
            # Query Cypher pour insérer les données dans Memgraph
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

            # Exécuter la requête avec les données transformées
            graph.run(query, data=data)
            logger.info(f"{data} inséré avec succès dans Memgraph !")

        except Exception as e:
            logger.error(f"Une erreur est survenue lors de l'insertion dans Memgraph : {e}")

    def kafka_consumer():
        # Configuration du consommateur Kafka
        consumer_config = {
            'bootstrap.servers': 'localhost:9092',  # Adresse du broker Kafka
            'group.id': 'my-group',  # ID du groupe de consommateurs
            'auto.offset.reset': 'earliest'  # Commencer à consommer depuis le début du topic
        }

        # Créer une instance du consommateur Kafka
        consumer = Consumer(consumer_config)
 
        # S'abonner au topic Kafka
        topic = "critiques-movie"  # Remplace avec ton topic Kafka
        consumer.subscribe([topic])

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

                    # Transformer les données avant de les insérer dans Memgraph
                    transformed_data = transformer_donnees(msg.value().decode('utf-8'))

                    if transformed_data:
                        inserer_donnees_memgraph(transformed_data)
                    else:
                        logger.error("Les données n'ont pas été transformées correctement.")
                else:
                    logger.error(f"Erreur lors de la consommation de messages : {msg.error()}")

        except KeyboardInterrupt:
            logger.info("Arrêt du consommateur Kafka")
        except Exception as e:
            logger.error(f"Une erreur inattendue est survenue : {e}")
        finally:
            consumer.close()

    # Appel du consommateur Kafka
    kafka_consumer()

except Exception as e:
    logger.error(f"Une erreur est survenue lors de la connexion à Memgraph : {e}")

finally:
    # Fermeture de la connexion à Memgraph
    if graph is not None:
        graph.close()
        logger.info("Connexion à Memgraph fermée.")
