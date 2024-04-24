from kafka import KafkaConsumer
import json
from itertools import combinations

# Define Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic_name = 'preprocesseddatatopic3'

# Create a Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Helper function to generate vertical data format
def vertical_data_format(transactions):
    itemsets = {}
    for idx, transaction in enumerate(transactions):
        for item in transaction['feature']:  # Adjust the field if necessary
            if item not in itemsets:
                itemsets[item] = set()
            itemsets[item].add(idx)
    return itemsets

# Eclat algorithm to find frequent itemsets
def eclat(itemsets, min_support):
    frequent_itemsets = []
    # Generate initial frequent itemsets
    for item, tidset in itemsets.items():
        if len(tidset) >= min_support:
            frequent_itemsets.append((item, tidset))

    # Recursive function to explore itemsets
    def explore_itemsets(prefix, tidset, items):
        for i, (item, otidset) in enumerate(items):
            new_tidset = tidset & otidset
            if len(new_tidset) >= min_support:
                result = prefix + [item]
                frequent_itemsets.append((result, new_tidset))
                explore_itemsets(result, new_tidset, items[i+1:])

    # Explore each itemset
    for i, (item, tidset) in enumerate(frequent_itemsets):
        explore_itemsets([item], tidset, frequent_itemsets[i+1:])
    
    return frequent_itemsets

# Buffer to collect transactions
transaction_buffer = []
min_support = 5  # Define your support threshold

# Process messages from Kafka
for message in consumer:
    transaction_buffer.append(message.value)
    print("Received data:", message.value)

    # Process transactions periodically
    if len(transaction_buffer) >= 100:  # Adjust batch size according to your needs
        print("Processing batch of transactions...")
        vdata = vertical_data_format(transaction_buffer)
        results = eclat(vdata, min_support)
        print("Frequent itemsets identified:", results)
        transaction_buffer = []  # Reset buffer after processing

