import json
from kafka import KafkaProducer

# Define Kafka producer settings
bootstrap_servers = 'localhost:9092'

# Define topic names for each consumer
topic_names = ['preprocesseddatatopic1', 'preprocesseddatatopic2', 'preprocesseddatatopic3']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to read data from JSON file and ignore lines causing JSONDecodeError
def read_data_from_json(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if line:
                try:
                    item = json.loads(line)
                    data.append(item)
                except json.JSONDecodeError as e:
                    print(f"Issue in line {line_number}: {str(e)}. Skipping this line.")
    return data

# Main function to send data to Kafka topics
def send_data_to_kafka(producer, topic_names, data):
    for topic_name in topic_names:
        for item in data:
            producer.send(topic_name, value=item)
            print(f"Sent data to Kafka topic '{topic_name}':", item)

# Path to the JSON data file
json_file_path = '/home/taha/kafka/Assignment_BDA/data.json'  # Replace with the actual path to your JSON file

# Read data from JSON file and ignore problematic lines
data = read_data_from_json(json_file_path)

# Send data to Kafka topics
send_data_to_kafka(producer, topic_names, data)

# Close the Kafka producer
producer.close()

