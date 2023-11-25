from confluent_kafka import Consumer, KafkaException
import json
import logging
from neo4j import GraphDatabase

# Configurer les logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define correct URI and AUTH arguments (no AUTH by default)
URI = "bolt://localhost:7687"
AUTH = ("", "")
 
try:
    with GraphDatabase.driver(URI, auth=AUTH) as client:
    # Check the connection
        if client.verify_connectivity():
            print("Connexion établie avec succès !")


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
            client.execute_query(
                query,
                {"data": data},
                database_="memgraph"
            )

            # with client.session(database="memgraph") as session:
            #     # Exécution de la requête avec les données transformées
            #     session.run(query, data=data)
            logger.info(f"{data} inséré avec succès dans Memgraph !")

        except Exception as e:
            logger.error(f"Une erreur est survenue lors de l'insertion dans Memgraph : {e}")

    def kafka_consumer():
        # Configuration du consommateur Kafka
        consumer_config = {
            'bootstrap.servers': 'localhost:9092',  # Adresse du broker Kafka
            'group.id': 'my_group',  # ID du groupe de consommateurs
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
                msg = consumer.poll(5.0)  # Consommer les messages, attendre 5 secondes pour de nouveaux messages

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
        except KafkaException as kafka_exception:
            logger.error(f"Erreur de connexion à Kafka : {kafka_exception}")
        except Exception as e:
            logger.error(f"Une erreur inattendue est survenue : {e}")
        finally:
            consumer.close()

    # Appel du consommateur Kafka
    kafka_consumer()

except Exception as e:
    logger.error(f"Une erreur inattendue est survenue lors de la connexion : {e}")
finally:
    if client is not None:
        client.close()
        logger.info("Connexion à Memgraph fermée.")
