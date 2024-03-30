# Read Data from CSV File in S3:
import pandas as pd
data = pd.read_csv('s3://Sales/sales_data.csv')

# Remove Rows with Duplicates, Null, Empty, Errors:
data.drop_duplicates(inplace=True)
data.dropna(inplace=True)

data.replace('', pd.NA, inplace=True)
data.dropna(inplace=True)


# Select Rows Matching Criteria:
selected_rows = data[(data['Total_Sales_Amount'] > 5600) & 
                     (data['Store_Location'] == 'Madrid') & 
                     (data['Payment_Method'] == 'Bank Transfer') & 
                     (data['Sales_Channel'] == 'Store')]


# Save Transformed Data to CSV:
selected_rows.to_csv('s3://Sales_Silver/transformed_sales_data.csv', index=False)

