
import sys
import os
import json
import logging
import time
from datetime import datetime 
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import requests 

sys.path.append('/home/Critiques-Films-Kafka-Memgraph/data') 
sys.path.append('/home/Critiques-Films-Kafka-Memgraph/u.data') 

from API import * 

# Set up Loggin Function
def setup_producer_logging():

    log_directory = "Log/Producer_Log_Files"
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer_logger = logging.getLogger(__name__)  

    return producer_logger

def create_kafka_topic(topic, admin_client, producer_logger):
    try:
        topic_spec = NewTopic(topic, num_partitions=1, replication_factor=1)

        admin_client.create_topics([topic_spec])

        separator = '-' * 30
        producer_logger.info(f"{topic} {separator} Created Successfully: ")

    except Exception as e:
        error_message = "Error creating Kafka topic: " + str(e)
        producer_logger.error(error_message)

def produce_to_Topics(movieTopic, producer_logger):
    try:
        producer = Producer({"bootstrap.servers": "localhost:9092"})  # Kafka broker address

        while True:
            try:
                # Use the streaming endpoint to get movie data
                movie_data_endpoint = "http://127.0.0.1:5000/movie_data"
                response = requests.get(movie_data_endpoint, stream=True)

                for line in response.iter_lines():
                    try:
                        if line:
                            movie_json = json.loads(line)

                            producer.produce(movieTopic, key="movie", value=json.dumps(movie_json).encode('utf-8'))
                            producer_logger.info(f"Movie Produced Successfully to {movieTopic}: {movie_json}")
                            producer.flush()

                    except ValueError as ve:
                        # Log the error if the JSON parsing fails
                        error_message = f"Error parsing JSON: {ve}"
                        producer_logger.error(error_message)

                    except Exception as ex:
                        # Log other validation errors
                        error_message = f"Error validating Kafka message: {ex}"
                        producer_logger.error(error_message)

                # Close the response after processing all lines
                response.close()

            except Exception as e:
                error_message = "Error getting movie data: " + str(e)
                producer_logger.error(error_message)

    except Exception as e:
        error_message = "Error producing to Kafka: " + str(e)
        producer_logger.error(error_message)

def runKafkaProducer(topic):
    
    producer_logger = setup_producer_logging()

    try:
        producer_logger.info("Kafka Producer started.")

        # Create a Kafka admin client
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})

        # Check if the topics exist, and create them if not
        for topic in [topic]:
            existing_topics = admin_client.list_topics().topics
            if topic not in existing_topics:
                create_kafka_topic(topic, admin_client, producer_logger)

        # Start producing to both topics simultaneously
        produce_to_Topics(topic, producer_logger)

        
    except KeyboardInterrupt:
        producer_logger.info("Kafka Producer Stopped")

    except Exception as e:
        error_message = "An unexpected error occurred in Kafka Producer: " + str(e)
        producer_logger.error(error_message)

topic = "critiques-movie"

runKafkaProducer(topic)