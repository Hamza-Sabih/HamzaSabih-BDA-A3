from kafka import KafkaConsumer
import json
from collections import defaultdict
import hashlib
from itertools import combinations  # Import this module for combinations


# Define Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic_name = 'preprocesseddatatopic2'

# Create a Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Helper function to hash itemsets
def hash_itemset(itemset, num_buckets):
    return int(hashlib.sha256(" ".join(sorted(itemset)).encode()).hexdigest(), 16) % num_buckets

# PCY Algorithm implementation
def pcy_algorithm(transactions, support_threshold, num_buckets):
    # First pass: count item frequencies and bucket counts
    item_counts = defaultdict(int)
    bucket_counts = defaultdict(int)

    for transaction in transactions:
        items = transaction['feature']  # Assuming 'feature' contains itemsets; adjust as needed
        for item in items:
            item_counts[item] += 1
        for pair in combinations(items, 2):
            bucket_index = hash_itemset(pair, num_buckets)
            bucket_counts[bucket_index] += 1

    # Identify frequent items and frequent buckets
    frequent_items = {item for item, count in item_counts.items() if count >= support_threshold}
    frequent_buckets = {bucket for bucket, count in bucket_counts.items() if count >= support_threshold}

    # Second pass: count pairs using only frequent buckets
    pair_counts = defaultdict(int)
    for transaction in transactions:
        items = [item for item in transaction['feature'] if item in frequent_items]
        for pair in combinations(items, 2):
            if hash_itemset(pair, num_buckets) in frequent_buckets:
                pair_counts[pair] += 1

    # Identify frequent pairs
    frequent_pairs = {pair for pair, count in pair_counts.items() if count >= support_threshold}
    return frequent_pairs

# Placeholder for storing transactions for batch processing
transaction_buffer = []

# Set parameters for the PCY algorithm
support_threshold = 5  # Define according to your data
num_buckets = 1000  # Define according to expected combinations

# Process messages
for message in consumer:
    transaction_buffer.append(message.value)
    print("Received data:", message.value)
    
    # Perform PCY on the collected transactions periodically
    if len(transaction_buffer) >= 100:  # Define the batch size according to your throughput needs
        print("Processing batch of transactions...")
        frequent_itemsets = pcy_algorithm(transaction_buffer, support_threshold, num_buckets)
        print("Frequent itemsets identified:", frequent_itemsets)
        transaction_buffer = []  # Clear buffer after processing

# Note: In practice, the consumer should run indefinitely, or manage offsets and commits appropriately.

