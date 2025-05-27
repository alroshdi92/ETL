# ETL
ETL Pipeline – Store Sales Data

🧾 Overview
This project demonstrates a complete ETL (Extract, Transform, Load) process using Python and MySQL to handle and load sales data from a retail store into a structured database.

🔧 Tools & Technologies
Python 🐍

Pandas 🧹

MySQL 🗃️

MySQL Connector

Jupyter/Thonny/VS Code

CSV Files

📥 1. Extract
Read raw data from a store_sales_1.csv file using pandas.

🔄 2. Transform
Handle missing values:

Imputed missing Unit_Price and CurrencyType using most frequent values.

Filled missing quantities with 1.

Dropped rows with missing CustomerID.

Cleaned and formatted the SaleDate column.

Calculated:

Total_Price = Qty × Unit_Price

Total_Price_OMR using currency exchange rates.

📤 3. Load
Created a normalized database schema with four related tables:

Product

Customer

Store

Sales

Established foreign key relationships.

Inserted clean data into the appropriate tables using Python and SQL.


