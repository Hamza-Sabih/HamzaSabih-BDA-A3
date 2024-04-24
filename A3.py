import json
import os
from tqdm import tqdm
import re
import string

# # Defining a function named sample_json that takes four parameters:
# # input_file: the path to the input JSON file
# # output_file: the path to the output JSON file
# # target_size_gb: the target size of the output file in gigabytes
# # filter_key: the key to filter records by, default is "also_buy"


def sample_json(input_file, output_file, target_size_gb, filter_key="also_buy"):
    # Convert the target size from gigabytes to bytes
    target_size_bytes = target_size_gb * 1024 ** 3

    # Initialize the current size of the output file in bytes
    current_size_bytes = 0

    # Open the input file in read mode and the output file in write mode
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding="utf-8") as outfile:
        # Loop over each line in the input file
        for line in tqdm(infile):
            # Load the JSON data from the current line
            record = json.loads(line)

            # Check if the filter key exists and is not empty in the current record
            if record.get(filter_key):
                # If it exists, write the record to the output file and add a new line
                outfile.write(json.dumps(record) + '\n')

                # Add the size of the current line to the current size of the output file
                current_size_bytes += len(line.encode('utf-8'))

            # If the current size of the output file is greater than or equal to the target size
            if current_size_bytes >= target_size_bytes:
                # Stop writing to the output file
                break

    # Print the final size of the output file in gigabytes
    print(f"Finished sampling. Output size: {current_size_bytes / 1024**3:.2f} GB")

sample_json('Sampled_Amazon_Meta.json', '1GB_Sampled_Amazon_Meta.json', 15)


#DATA PREPROCESSING
def display_file_preview(file_path, num_lines=10):
    with open(file_path, 'r', encoding='utf-8') as file:
        for _ in range(num_lines):
            line = file.readline().strip()
            print(line)





# # Specify the path to the large JSON file
# file_path = '1GB_Sampled_Amazon_Meta.json'

# # Display a preview of the first 10 lines
# display_file_preview(file_path, num_lines=10)

# import re
# import string

# Function to load the sampled Amazon dataset
def load_dataset(input_file):
    with open(input_file, 'r', encoding='utf-8') as infile:
        dataset = [json.loads(line) for line in infile]
    return dataset


def preprocess_dataset(dataset):
    cleaned_dataset = []
    for entry in dataset:
        # Clean all string fields by removing HTML tags and punctuation
        for key, value in entry.items():
            if isinstance(value, str):
                cleaned_text = remove_html_tags(value)
                cleaned_text = remove_punctuation(cleaned_text)
                entry[key] = cleaned_text

        # Conditionally create a cleaned record if certain criteria are met
        if entry.get('asin') and entry.get('title') and not any('<a href=' in feature for feature in entry.get('feature', [])):
            cleaned_record = {
                "asin": entry.get("asin", ""),
                "title": entry.get("title", ""),
                "feature": entry.get("feature", []),
                "description": entry.get("description", ""),
                "price": entry.get("price", ""),
                "imageURL": entry.get("imageURL", []),
                "brand": entry.get("brand", "")
            }
            cleaned_dataset.append(cleaned_record)

    return cleaned_dataset

# Function to remove HTML tags from text
def remove_html_tags(text):
    clean_text = re.sub(r'<[^>]+>', '', text)
    return clean_text

# Function to remove punctuation from text
def remove_punctuation(text):
    clean_text = ''.join([char for char in text if char not in string.punctuation])
    return clean_text

# Function to save preprocessed data to a new JSON file
def save_preprocessed_data(preprocessed_data, output_file):
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for record in preprocessed_data:
            outfile.write(json.dumps(record) + '\n')

# Load the sampled Amazon dataset
sampled_dataset = load_dataset('Sampled_Amazon_Meta.json')

# Preprocess the data
preprocessed_dataset = preprocess_dataset(sampled_dataset)

# Save preprocessed data to a new JSON file
save_preprocessed_data(preprocessed_dataset, '1GB_Sampled_Amazon_Meta.json')


#Specify the path to the large JSON file
# file_path = 'Preprocessed_Amazon_Meta.json'
# display_file_preview(file_path, num_lines=10)