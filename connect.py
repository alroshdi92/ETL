import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="sales_db"
)

cursor = conn.cursor()

df = pd.read_csv("store_sales_1.csv")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Sales (
    SaleID INT AUTO_INCREMENT PRIMARY KEY,
    StoreID INT,
    ProductID INT,
    CustomerID INT,
    Qty INT,
    Unit_Price FLOAT,
    Total_Price FLOAT,
    Total_Price_OMR FLOAT,
    SaleDate DATETIME,
    CurrencyType VARCHAR(10)
)
""")


# Create Product table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Product (
    ProductID INT AUTO_INCREMENT PRIMARY KEY,
    ProductName VARCHAR(255) NOT NULL
);
""")

# Create Customer table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Customer (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CustomerName VARCHAR(255),
    ContactInfo VARCHAR(255)
);
""")

# Create Store table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Store (
    StoreID VARCHAR(50) PRIMARY KEY,
    StoreName VARCHAR(255),
    Location VARCHAR(255)
);
""")

conn.commit()

cursor.close()
conn.close()
