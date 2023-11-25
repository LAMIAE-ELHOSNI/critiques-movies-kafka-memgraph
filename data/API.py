from flask import Flask, jsonify, request
import pandas as pd
import sys
import time
import logging
from datetime import datetime
import os
import json 
import requests 

sys.path.append('/home/Critiques-Films-Kafka-Memgraph/data')

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
        u_data = pd.read_csv('/home/Critiques-Films-Kafka-Memgraph/data/u.data', sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])
        u_item = pd.read_csv('/home/Critiques-Films-Kafka-Memgraph/data/u.item', sep='|', encoding='latin-1', header=None, names=['movieId', 'title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'])
        u_user = pd.read_csv('/home/Critiques-Films-Kafka-Memgraph/data/u.user', sep='|', names=['userId', 'age', 'gender', 'occupation', 'zipcode'])
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
                json_str = json.dumps(json_data) + '\n'
                yield json_str.encode('utf-8')
                time.sleep(2)
                api_logger.info(f"Returned message: {json_str}")

        api_logger.info("API execution completed.")
        return app.response_class(generate(), content_type='application/json', status=200, mimetype='application/json', direct_passthrough=True)

    except Exception as e:
        api_logger.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


if __name__ == '__main__':
    app.run(debug=True)