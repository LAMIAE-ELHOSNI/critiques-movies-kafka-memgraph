from flask import Flask, jsonify, request
import pandas as pd
import sys
import time
import logging
from datetime import datetime
import os
import json 
import requests 

sys.path.append('/home/hadoop/Data-Streaming-and-Analysis-Project/data')

# Set up Logging Function
def setup_api_logging():
    log_directory = "Log/API_Log_Files"
    os.makedirs(log_directory, exist_ok=True)

    # Create a log file with a timestamp in its name
    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    # Configure logging to write to the log file
    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Get the logger for this script
    api_logger = logging.getLogger(__name__)

    return api_logger

app = Flask(__name__)

api_logger = setup_api_logging()

# Function to read data files
def read_data_files():
    try:
        u_data = pd.read_csv('/home/hadoop/Data-Streaming-and-Analysis-Project/data/u.data', sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])
        u_item = pd.read_csv('/home/hadoop/Data-Streaming-and-Analysis-Project/data/u.item', sep='|', encoding='latin-1', header=None, names=['movieId', 'title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'])
        u_user = pd.read_csv('/home/hadoop/Data-Streaming-and-Analysis-Project/data/u.user', sep='|', names=['userId', 'age', 'gender', 'occupation', 'zipcode'])
        return u_data, u_item, u_user
    except Exception as e:
        api_logger.error(f"Error reading data files: {e}")
        raise

# Function to extract genres for each movie
def extract_genres(row):
    genres = ['unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    movie_genres = [genre for genre, val in zip(genres, row[5:]) if val == 1]
    return movie_genres

# Function to create JSON entry
def create_json_entry(row):
    return {
        "userId": str(row['userId']),
        "movie": {
            "movieId": str(row['movieId']),
            "title": row['title'],
            "genres": row['genres']
        },
        "rating": str(row['rating']),
        "timestamp": str(row['timestamp'])
    }

@app.route('/movie_data', methods=['GET'])
def get_movie_data():
    try:
        # Set up API logger
        api_logger.info("API execution started.")

        # Read data files
        u_data, u_item, u_user = read_data_files()
        api_logger.info("Data files read successfully.")

        # Apply genre extraction function to each row in u_item
        u_item['genres'] = u_item.apply(extract_genres, axis=1)

        # Merge relevant data
        merged_data = pd.merge(u_data, u_item[['movieId', 'title', 'genres']], on='movieId')
        merged_data = pd.merge(merged_data, u_user[['userId', 'age', 'gender', 'occupation']], on='userId')

        # Convert to JSON format and return as a streaming response with a delay
        def generate():
            for _, row in merged_data.iterrows():
                json_data = create_json_entry(row)
                json_str = json.dumps(json_data)  # Convert the dictionary to a JSON-formatted string
                yield json_str + '\n'  # Ensure each JSON object is on a new line
                time.sleep(2)
                api_logger.info(f"Returned message: {json_str}")

        api_logger.info("API execution completed.")
        return app.response_class(generate(), content_type='application/json')
    except Exception as e:
        api_logger.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


def getData():
    response = requests.get("http://127.0.0.1:5000/movie_data")
    yield response


if __name__ == '__main__':
    app.run(debug=True)













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

sys.path.append('/home/hadoop/Data-Streaming-and-Analysis-Project/data') 
sys.path.append('/home/hadoop/Data-Streaming-and-Analysis-Project/data/u.data') 

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

        moviedata = getData()

        while True:
            movie_data = next(moviedata)

            movie_json = json.loads(movie_data)
            # Check if "release_date" is present and not null in the movie object
            try:
                # Produce to the movies topic
                producer.produce(topic, key="movie", value=json.dumps(movie_json))
                producer_logger.info(f"Movie Produced Successfully to {topic}: ")

                # Flush only if everything is successful
                producer.flush()

            except ValueError as ve:
                # Log the error if the date formatting fails
                error_message = f"Error formatting release date: {ve}"
                producer_logger.error(error_message)

            except Exception as ex:
                # Log other validation errors
                error_message = f"Error validating Kafka message: {ex}"
                producer_logger.error(error_message)

            # Add a delay to control the rate of data production
            time.sleep(2)

    except StopIteration:
        producer_logger.info("Movie data generator exhausted. Stopping Kafka Producer.")

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

topic = "testfilm"

runKafkaProducer(topic)