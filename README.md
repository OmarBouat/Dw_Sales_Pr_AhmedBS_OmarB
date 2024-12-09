# Sales Data ETL Project for Sales Data Warehouse

## Overview
This project extracts, transforms, and loads data from a store sales dataset into a data warehouse named `sales`. The ETL process prepares the data for analysis by creating dimension tables (Date, Product, Customer, Ship) and a fact table (`Sales_Fact`).

## Prerequisites
- Python (version 3.x)
- pandas library

## Directory Structure
- `data/`: Original Superstore CSV file.
- `outputs/`: Generated CSV files for each dimension and the fact table.
- `scripts/`: Main Python script for the ETL process.

## ETL Process Steps
1. **Loading the Dataset**:
   - Load the dataset from a specified file path.
   - Parse dates for 'Order Date' and 'Ship Date'.

2. **Creating Dimension Tables**:
   - **Date Dimension (`Date_Dim`)**: Includes day, month, year, day of the week, week number, quarter, and semester.
   - **Product Dimension (`Product_Dim`)**: Includes product details such as ID, name, category, sub-category, and version.
   - **Customer Dimension (`Customer_Dim`)**: Includes customer details such as ID, name, address, segment, and region.
   - **Ship Dimension (`Ship_Dim`)**: Includes shipping modes.

3. **Creating Fact Table (`Sales_Fact`)**:
   - Contains transactional data including order ID, customer ID, product ID, sales, quantity, discount, and profit.
   - Maps surrogate keys for customer, product, and date dimensions.

4. **Mapping IDs**:
   - Surrogate keys are created for each dimension.
   - IDs are mapped between the fact table and dimension tables.
   - Handle any unmapped customer or product IDs.

5. **Output**:
   - Save each dimension and the fact table as a separate CSV file in the `outputs/` directory.

## Getting Started
1. Install the required libraries:
   ```bash
   pip install pandas
