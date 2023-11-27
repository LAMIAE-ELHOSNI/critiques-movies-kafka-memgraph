from confluent_kafka import Consumer
import json
import logging
from neo4j import GraphDatabase

# Configurer les logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Établir la connexion à Neo4j
uri = "bolt://localhost:7687"
username = ""  # Votre nom d'utilisateur
password = ""  # Votre mot de passe
driver = GraphDatabase.driver(uri, auth=(username, password))

try:
    def transformer_donnees(message):
        try:
            # Convertir le message JSON en dictionnaire Python
            data = json.loads(message)

            # Adapter la structure des données au format attendu par Neo4j
            transformed_data = {
                "userId": data.get("userId", ""),
                "movieId": data.get("movie", {}).get("movieId", ""),
                "title": data.get("movie", {}).get("title", ""),
                "genres": data.get("movie", {}).get("genres", []),
                "rating": data.get("rating", ""),
                "timestamp": data.get("timestamp", "")
            }

            return transformed_data

        except Exception as e:
            logger.error(f"Erreur lors de la transformation des données : {e}")
            return None

    def inserer_donnees_neo4j(data):
        try:
            with driver.session() as session:
                # Query Cypher pour insérer les données dans Neo4j
                query = """
                    MERGE (user:User {userId: $userId})
                    MERGE (movie:Movie {movieId: $movieId})
                    ON CREATE SET movie.title = $title

                    FOREACH (genre IN $genres |
                        MERGE (g:Genre {name: genre})
                        MERGE (movie)-[:IS_GENRE]->(g)
                    )

                    MERGE (user)-[r:RATED]->(movie)
                    SET r.rating = toInteger($rating), r.timestamp = toInteger($timestamp)
                """

                session.run(query, **data)
                logger.info(f"{data} inséré avec succès dans Neo4j !")

        except Exception as e:
            logger.error(f"Une erreur est survenue lors de l'insertion dans Neo4j : {e}")

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
                msg = consumer.poll(5.0)  # Consommer les messages, attendre jusqu'à 5 secondes pour de nouveaux messages

                if msg is None:
                    logger.info("Aucun nouveau message reçu. Sortie.")
                    break
                elif not msg.error():
                    # Loguer les messages reçus depuis Kafka
                    logger.info(f"Message reçu depuis Kafka : {msg.value().decode('utf-8')}")

                    # Transformer les données avant de les insérer dans Neo4j
                    transformed_data = transformer_donnees(msg.value().decode('utf-8'))

                    if transformed_data:
                        inserer_donnees_neo4j(transformed_data)
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
    logger.error(f"Une erreur est survenue lors de la connexion à Neo4j : {e}")

finally:
    # Fermeture de la connexion à Neo4j
    if driver is not None:
        driver.close()
        logger.info("Connexion à Neo4j fermée.")
