import pandas as pd
import json

# Load JSON data
with open('data.json', 'r') as file:
    data = json.load(file)

# Convert JSON to DataFrame
df = pd.DataFrame(data)

# Write DataFrame to CSV
df.to_csv('data.csv', index=False)
