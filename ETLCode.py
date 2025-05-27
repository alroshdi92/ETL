import pandas as pd
from sklearn.impute import SimpleImputer
import mysql.connector

# 1. Load and clean data
df = pd.read_csv("store_sales_1.csv")

print("Before cleaning, null counts:\n", df.isnull().sum())

# Impute most frequent for Unit_Price and CurrencyType
imputer = SimpleImputer(strategy='most_frequent')
for col in ['Unit_Price', 'CurrencyType']:
    if col in df.columns and df[col].isnull().any():
        df[col] = imputer.fit_transform(df[[col]]).ravel()

# Fill null Qty with 1
if 'Qty' in df.columns and df['Qty'].isnull().any():
    df['Qty'] = df['Qty'].fillna(1)

# Drop rows with null CustomerID (required)
df.dropna(subset=['CustomerID'], inplace=True)

# Convert data types
df['Qty'] = df['Qty'].astype(int)
df['Unit_Price'] = df['Unit_Price'].astype(float)

# Convert SaleDate to datetime
df['SaleDate'] = pd.to_datetime(df['SaleDate'], errors='coerce')

# drop rows with invalid dates
#df.dropna(subset=['SaleDate'], inplace=True)

# Format date to 'YYYY-MM-DD' string for MySQL DATE column
df['SaleDate'] = df['SaleDate'].dt.strftime('%Y-%m-%d')

# Clean CurrencyType text
df['CurrencyType'] = df['CurrencyType'].str.strip().str.upper()

# Calculate Total_Price
df['Total_Price'] = df['Qty'] * df['Unit_Price']

# Exchange rates for conversion to OMR
exchange_rates = {
    "USD": 0.385,
    "EUR": 0.41,
    "OMR": 1.0
}

df['Total_Price_OMR'] = df.apply(
    lambda row: row['Total_Price'] * exchange_rates.get(row['CurrencyType'], 1.0),
    axis=1
)

print("After cleaning, null counts:\n", df.isnull().sum())

print("Data preview:\n", df.head())


# 2. Connect to MySQL and create tables
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="sales_db"
)
cursor = conn.cursor()

# Drop existing tables to start fresh
cursor.execute("DROP TABLE IF EXISTS Sales")
cursor.execute("DROP TABLE IF EXISTS Product")
cursor.execute("DROP TABLE IF EXISTS Customer")
cursor.execute("DROP TABLE IF EXISTS Store")

# Create tables
cursor.execute("""
CREATE TABLE Product (
    ProductID INT AUTO_INCREMENT PRIMARY KEY,
    ProductName VARCHAR(255) UNIQUE NOT NULL
);
""")

cursor.execute("""
CREATE TABLE Customer (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CustomerName VARCHAR(255),
    ContactInfo VARCHAR(255)
);
""")

cursor.execute("""
CREATE TABLE Store (
    StoreID VARCHAR(50) PRIMARY KEY,
    StoreName VARCHAR(255),
    Location VARCHAR(255)
);
""")

cursor.execute("""
CREATE TABLE Sales (
    SaleID INT AUTO_INCREMENT PRIMARY KEY,
    StoreID VARCHAR(50),
    ProductID INT,
    CustomerID VARCHAR(50),
    Qty INT,
    Unit_Price FLOAT,
    Total_Price FLOAT,
    Total_Price_OMR FLOAT,
    SaleDate DATE,
    CurrencyType VARCHAR(10),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (StoreID) REFERENCES Store(StoreID)
);
""")

conn.commit()

# 3. Insert distinct Products
products = df['ProductName'].dropna().unique()
for product in products:
    cursor.execute("SELECT ProductID FROM Product WHERE ProductName = %s", (product,))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO Product (ProductName) VALUES (%s)", (product,))
conn.commit()

# 4. Insert distinct Customers (only CustomerID as CSV lacks details)
customers = df['CustomerID'].dropna().unique()
for cust in customers:
    cursor.execute("SELECT CustomerID FROM Customer WHERE CustomerID = %s", (cust,))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO Customer (CustomerID) VALUES (%s)", (cust,))
conn.commit()

# 5. Insert distinct Stores (only StoreID as CSV lacks details)
stores = df['StoreID'].dropna().unique()
for store in stores:
    cursor.execute("SELECT StoreID FROM Store WHERE StoreID = %s", (store,))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO Store (StoreID) VALUES (%s)", (store,))
conn.commit()

# 6. Map ProductName to ProductID for FK insertion
cursor.execute("SELECT ProductID, ProductName FROM Product")
product_map = {name: pid for pid, name in cursor.fetchall()}

# 7. Insert into Sales table
for _, row in df.iterrows():
    product_id = product_map.get(row['ProductName'])
    if product_id is None:
        continue  # skip if product not found
    
    cursor.execute("""
        INSERT INTO Sales
        (StoreID, ProductID, CustomerID, Qty, Unit_Price, Total_Price, Total_Price_OMR, SaleDate, CurrencyType)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['StoreID'], product_id, row['CustomerID'], row['Qty'], row['Unit_Price'],
        row['Total_Price'], row['Total_Price_OMR'], row['SaleDate'], row['CurrencyType']
    ))

conn.commit()

cursor.close()
conn.close()

print("Data inserted successfully.")
