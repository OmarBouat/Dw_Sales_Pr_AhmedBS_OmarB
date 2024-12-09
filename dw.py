import pandas as pd

file_path = r"C:\Users\omarb\Downloads\Superstore.csv"
data = pd.read_csv(file_path, encoding="latin1")

data['Order Date'] = pd.to_datetime(data['Order Date'], format='%d-%m-%Y', errors='coerce')
data['Ship Date'] = pd.to_datetime(data['Ship Date'], format='%d-%m-%Y', errors='coerce')

if data['Order Date'].isna().sum() > 0 or data['Ship Date'].isna().sum() > 0:
    print("Warning: Some dates could not be parsed.")

unique_dates = pd.concat([data['Order Date'], data['Ship Date']]).drop_duplicates()
date_dim = pd.DataFrame({'full_date': unique_dates})
date_dim['day'] = date_dim['full_date'].dt.day
date_dim['month'] = date_dim['full_date'].dt.month
date_dim['year'] = date_dim['full_date'].dt.year
date_dim['day_of_week'] = date_dim['full_date'].dt.dayofweek + 1
date_dim['day_name'] = date_dim['full_date'].dt.day_name()
date_dim['week_number'] = date_dim['full_date'].dt.isocalendar().week
date_dim['quarter'] = date_dim['full_date'].dt.quarter
date_dim['semester'] = (date_dim['month'] - 1) // 6 + 1
date_dim.reset_index(drop=True, inplace=True)
date_dim.insert(0, 'surr_id', date_dim.index + 1)

product_dim = data[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates()
product_dim.reset_index(drop=True, inplace=True)
product_dim.insert(0, 'surr_id', product_dim.index + 1)
product_dim.rename(columns={
    'Product ID': 'product_id',
    'Category': 'category',
    'Sub-Category': 'sub_category',
    'Product Name': 'product_name'
}, inplace=True)
product_dim['start_date'] = None
product_dim['end_date'] = None
product_dim['version'] = 1
product_dim['current'] = True
product_dim['last_update'] = pd.Timestamp.now()

product_dim = product_dim.drop_duplicates(subset=['product_id'])

customer_dim = data[['Customer ID', 'Customer Name', 'Segment', 'City', 'State', 'Postal Code', 
                     'Region', 'Country']].drop_duplicates()
customer_dim.reset_index(drop=True, inplace=True)
customer_dim.insert(0, 'surr_id', customer_dim.index + 1)
customer_dim.rename(columns={
    'Customer ID': 'customer_id',
    'Customer Name': 'customer_name',
    'Postal Code': 'postal_code',
    'Segment': 'segment',
    'State': 'state_name'
}, inplace=True)
customer_dim['capital'] = None
customer_dim['latitude'] = None
customer_dim['longitude'] = None

customer_dim = customer_dim.drop_duplicates(subset=['customer_id'])

ship_dim = data[['Ship Mode']].drop_duplicates()
ship_dim.reset_index(drop=True, inplace=True)
ship_dim.insert(0, 'surr_id', ship_dim.index + 1)
ship_dim.rename(columns={'Ship Mode': 'ship_mode'}, inplace=True)

sales_fact = data.copy()
sales_fact = sales_fact.rename(columns={
    'Row ID': 'order_line',
    'Order ID': 'order_id',
    'Customer ID': 's_cust_id',
    'Product ID': 's_prod_id',
    'Order Date': 's_date_id',
    'Ship Mode': 's_ship_id'
})
sales_fact['s_date_id'] = sales_fact['s_date_id'].map(
    date_dim.set_index('full_date')['surr_id']
)
sales_fact['s_ship_id'] = sales_fact['s_ship_id'].map(
    ship_dim.set_index('ship_mode')['surr_id']
)
sales_fact['s_cust_id'] = sales_fact['s_cust_id'].map(
    customer_dim.set_index('customer_id')['surr_id']
)

unmapped_customers = sales_fact[sales_fact['s_cust_id'].isna()]
if not unmapped_customers.empty:
    print("Warning: The following customer IDs could not be mapped:")
    print(unmapped_customers['s_cust_id'].unique())

product_dim = product_dim.drop_duplicates(subset=['product_id'])

sales_fact['s_prod_id'] = sales_fact['s_prod_id'].map(
    product_dim.set_index('product_id')['surr_id']
)

unmapped_products = sales_fact[sales_fact['s_prod_id'].isna()]
if not unmapped_products.empty:
    print("Warning: The following product IDs could not be mapped:")
    print(unmapped_products['s_prod_id'].unique())

sales_fact = sales_fact[['order_line', 'order_id', 's_ship_id', 's_cust_id', 's_prod_id', 
                         's_date_id', 'Sales', 'Quantity', 'Discount', 'Profit']]

output_dir = r"C:\Users\omarb\Downloads"

date_dim.to_csv(f"{output_dir}/Date_Dim.csv", index=False)
product_dim.to_csv(f"{output_dir}/Product_Dim.csv", index=False)
customer_dim.to_csv(f"{output_dir}/Customer_Dim.csv", index=False)
ship_dim.to_csv(f"{output_dir}/Ship_Dim.csv", index=False)
sales_fact.to_csv(f"{output_dir}/Sales_Fact.csv", index=False)

print("ETL Process Completed. Dimension and Fact tables have been saved.")

print(data.head())
print(date_dim.head())
print(product_dim.head())
print(customer_dim.head())
print(ship_dim.head())
print(sales_fact.head())
