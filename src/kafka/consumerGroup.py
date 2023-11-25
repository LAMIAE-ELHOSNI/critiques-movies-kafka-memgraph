from confluent_kafka.admin import AdminClient

# Configuration pour l'AdminClient
admin_config = {'bootstrap.servers': 'localhost:9092'}  # Assurez-vous que c'est le bon broker

# Création de l'AdminClient
admin_client = AdminClient(admin_config)

# Liste des groupes de consommateurs
group_metadata = admin_client.list_groups()

# Affichage des groupes de consommateurs
for group in group_metadata:
    print(group.id)
