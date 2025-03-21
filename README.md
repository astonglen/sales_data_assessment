#  Sales Data Assessment

##  Project Overview
This project processes and validates sales data from two CSV files (`order_region_a.csv` and `order_region_b.csv`). It performs the following tasks:

- Extracts and preprocesses the data.  
- Combines the datasets.  
- Loads the combined data into an SQLite database.  
- Runs SQL queries for validation and reporting.  
- Displays the results of the validation queries.  

---

## Project Structure

/Sales_data_assessment

 ├── data                     # Folder containing data files

 │     ├── order_region_a.csv       # CSV file for region A

 │     ├── order_region_b.csv       # CSV file for region B

 │     ├── combined_sales_data.csv  # Combined data after processing

 │     ├── sales_data.db            # SQLite database file

 ├── SQLqueries.sql           # SQL queries used for validation

 ├── main.py                  # Python script for data processing and validation

 ├── README.md                # Project documentation

 ├── requirements.txt         # Python dependencies

 └── .gitignore               # Files to ignore during Git commits
 

# How to Run the Program
# 1. Clone the Repository
If you haven't cloned the repository yet, do the following:

git clone https://github.com/astonglen/sales_data_assessment.git 

cd sales_data_assessment

# 2. Install Dependencies
Create a virtual environment (optional but recommended):

python -m venv .venv

# Install the required packages:

pip install -r requirements.txt

# 3. Database Setup
The program uses SQLite as the database.
You don’t need to manually create the database—the script will handle it automatically:

It creates the sales_data.db file inside the data folder.
It creates the sales_data table with the required schema.

# 4. Running the Program
Run the script using:

python main.py

