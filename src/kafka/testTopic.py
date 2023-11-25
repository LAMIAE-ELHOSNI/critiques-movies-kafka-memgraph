from confluent_kafka.admin import AdminClient

# Configuration pour l'AdminClient
admin_config = {'bootstrap.servers': 'localhost:9092'}  # Assurez-vous que c'est le bon broker

# Création de l'AdminClient
admin_client = AdminClient(admin_config)

# Liste des topics
topic_metadata = admin_client.list_topics()

# Vérification de l'existence du topic
if 'critiques-movie' in topic_metadata.topics:
    print("Le topic 'critiques-movie' existe.")
else:
    print("Le topic 'critiques-movie' n'existe pas.")
