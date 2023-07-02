import os
import json
import pandas as pd

# Specify the path to save the config file
config_file_path = "C:/Users/gayat/OneDrive/Desktop/file.cfg"

# Read the config data from the JSON file
with open(config_file_path, 'r') as json_file:
    config_data = json.load(json_file)

# Get the file names from the config data
files = list(config_data.values())

root_directory = "/"

output_folder = "C:/Users/gayat/OneDrive/Desktop/archive/out/"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Recursive function to search for the files
def search_files(directory):
    matched_files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            if filename in files:
                matched_files.append(os.path.join(root, filename))
                df = pd.read_csv(os.path.join(root, filename))
                output_file = os.path.join(output_folder, f"output_{filename}")
                df.to_csv(output_file, index=False)

    return matched_files

# Search for the files in the root directory
matched_files = search_files(root_directory)
print("Files found:", matched_files, "and written to output folder:", output_folder)

