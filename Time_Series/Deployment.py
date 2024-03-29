import pandas as pd

# Step 1: Read the CSV file
file_path = r'C:\Users\julio\OneDrive\Documentos\GitHub\Project_Data\Data\Time_Series\temperature_data.csv'  # Replace "your_file.csv" with the actual file path
df_temperature = pd.read_csv(file_path)
df_temperature.head(23)


selected_rows = df_temperature[(df_temperature['Year'] >= 2015)]
selected_rows.head(15)


import matplotlib.pyplot as plt

time_index = selected_rows.iloc[:, 0]  # Assuming the first column is the time index
data_columns = selected_rows.iloc[:, 1:]  # Assuming the rest of the columns are data

# Step 3: Plot the time series
plt.figure(figsize=(10, 6))
for column in data_columns:
    plt.plot(time_index, selected_rows[column], label=column)

plt.xlabel('Time')
plt.ylabel('Value')
plt.title('Time Series')
plt.legend()
plt.grid(True)
plt.show()