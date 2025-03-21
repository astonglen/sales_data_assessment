import pandas as pd
import os
import sqlite3

# Directory path
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "sales_data.db")
# File paths
file_a = os.path.join(DATA_DIR, "order_region_a.csv")
file_b = os.path.join(DATA_DIR, "order_region_b.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "combined_sales_data.csv")

def extract_data(file):
    try:
        df = pd.read_csv(file)
        return df
    except Exception as e:
        print(f"\nError reading {file}: {e}")
        return pd.DataFrame()

def pre_processing(df, region):
    try:
        df['QuantityOrdered'] = pd.to_numeric(df['QuantityOrdered'], errors='coerce').fillna(0)
        df['ItemPrice'] = pd.to_numeric(df['ItemPrice'], errors='coerce').fillna(0)
        df['PromotionDiscount'] = pd.to_numeric(df['PromotionDiscount'], errors='coerce').fillna(0)

        # 2. Add total_sales
        df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']

        # 3. Add region column
        df['region'] = region

        # 4. Remove duplicates by OrderId
        df.drop_duplicates(subset='OrderId', keep='first', inplace=True)

        # 5. Calculate net_sale
        df['net_sale'] = df['total_sales'] - df['PromotionDiscount']

        # 6. Filter out negative or zero sales
        df = df[df['net_sale'] > 0]

        return df

    except Exception as e:
        print(f"Error processing: {e}")
        return pd.DataFrame()

def create_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS sales_data;")

        # Create table with the necessary schema
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_data (
            OrderId TEXT NOT NULL,
            region TEXT NOT NULL,
            OrderItemId TEXT,
            QuantityOrdered INTEGER,
            ItemPrice REAL,
            PromotionDiscount REAL,
            total_sales REAL,
            net_sale REAL,
            PRIMARY KEY (OrderId, region)  -- Composite primary key
        );
        ''')
        conn.commit()
        conn.close()

        print("\nTable 'sales_data' created successfully.")
    except Exception as e:
        print(f"\nError creating table: {e}")

def load_data_to_sqlite(csv_file, db_path, table_name="sales_data"):
    """Load the transformed CSV data into SQLite."""
    try:
        # Read the CSV into a DataFrame
        df = pd.read_csv(csv_file)
        valid_columns = [
            "OrderId", "OrderItemId", "QuantityOrdered", "ItemPrice",
            "PromotionDiscount", "total_sales", "region", "net_sale"
        ]

        df = df[[col for col in valid_columns if col in df.columns]]

        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)

        # Insert data into table
        df.to_sql(table_name, conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

        print(f"\nData successfully loaded into '{table_name}' in '{db_path}'")
        print(f"Total records inserted: {len(df)}")

    except Exception as e:
        print(f"\nError loading data into SQLite: {e}")

def validate_data():
    print("\nRunning data validation...")

    #a Count the total number of records
    print("\nTotal number of records:")
    query = "SELECT COUNT(*) AS total_records FROM sales_data;"
    total_records = run_query(query)
    print(f"Total records: {total_records[0][0]}")

    #b Find the total sales amount by region
    print("\nTotal sales amount by region:")
    query = """
    SELECT region, SUM(net_sale) AS total_sales
    FROM sales_data
    GROUP BY region;
    """
    region_sales = run_query(query)
    for region, sales in region_sales:
        print(f"Region {region}: {sales}")

    #c Find the average sales amount per transaction
    print("\nAverage sales amount per transaction:")
    query = """
    SELECT AVG(net_sale) AS avg_sales_per_transaction
    FROM sales_data;
    """
    avg_sales = run_query(query)
    print(f"Average sales per transaction: {avg_sales[0][0]}")

    # d) Ensure there are no duplicate OrderId values (with the same region)
    print("\nChecking for duplicate OrderId values within the same region:")
    query = """
    SELECT 
        OrderId, 
        region, 
        COUNT(*) AS occurrences
    FROM sales_data
    GROUP BY OrderId, region
    HAVING occurrences > 1;
    """
    duplicates = run_query(query)

    if duplicates:
        print("\nDuplicates found:")
        for order_id, region, count in duplicates:
            print(f"{order_id} | {region} | {count} occurrences")
    else:
        print("No duplicates found.")


def run_query(query):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()

        conn.close()
        return rows

    except Exception as e:
        print(f"\nError executing query: {e}")
        return []


def main():
    print("\nLoading and processing data...")

    df_a = extract_data(file_a)
    df_b = extract_data(file_b)

    df_a = pre_processing(df_a, "A")
    df_b = pre_processing(df_b, "B")

    if df_a.empty and df_b.empty:
        print("\nNo valid data to combine. Exiting.")
        return

    print("\nRegion A Data Preview:")
    print(df_a.head())

    print("\nRegion B Data Preview:")
    print(df_b.head())

    #1 combined data
    combined_df = pd.concat([df_a, df_b], ignore_index=True)
    print("\nLength of Combined data:", len(combined_df))

    output_file = os.path.join(DATA_DIR, "combined_sales_data.csv")
    combined_df.to_csv(output_file, index=False)

    create_table()

    # Load the transformed CSV data into SQLite
    load_data_to_sqlite(output_file, DB_PATH)

    validate_data()

if __name__ == "__main__":
    main()