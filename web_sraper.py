from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime
import pypyodbc
import logging

# Configure logging
logging.basicConfig(filename="ccil_scraper.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Connect to Azure SQL
def connect_to_db():
    try:
        print('attempting to connect to database..')
        conn = pypyodbc.connect(
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:modular-server.database.windows.net,1433;Database=modular;Uid=CloudSA1c5b822c;Pwd={Givemeinternship!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        ) #connection string that was obtained from azure server.
        print('returning database object...')
        return conn
    except pypyodbc.Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

#scraping ccil market data

#selenium was used using a headless browser so that it can run on the virtual machine. beautiful soup cannot be used as the data from the table is loaded dynamically based on javascript.
def scrape_ccil():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://www.ccilindia.com/web/ccil/rbi-nds-om1"
    driver.get(url)

    time.sleep(5)

    rows = driver.find_elements("xpath", "//table/tbody/tr") #extract all the relevant tables.
    
    data = []
    for row in rows:
        cols = row.find_elements("tag name", "td")
        if len(cols) < 10:  
            continue
        data.append([col.text.strip() for col in cols])

    driver.quit()

    if not data:
        logging.warning("No data scraped from the website.")
        return None

    df = pd.DataFrame(data, columns=["Security", "Trades", "TTA", "Open", "High", "Low", "LTP", "Signal", "T/G", "LTY"])
    df["Timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df #return the dataframe extracted from the most recent page.

#this function checks if the data that was scraped already exists in the database. it checks the first row of the scraped data, and compares it to the first row of the database. if the records match, it means that nothing has been updated after one hour, and nothing needs to be inserted into the database.
def check_and_insert_data(cursor, new_first_row, all_new_data, current_timestamp):
    """
    Checks if the first row in the new table is different from the first row in the database.
    If different, inserts all new data.
    :param new_first_row: List containing the first row from the new table
    :param all_new_data: List of all new rows (to be inserted if the first row is different)
    :param current_timestamp: The timestamp for this batch of data
    """

    #extract security description from the first row
    security = new_first_row[0] 

    #retrieve the most recent first row from the database
    cursor.execute("""
        SELECT TOP 1 * FROM mktdata
        WHERE Security = ?
        ORDER BY Timestamp DESC
    """, (security,))
    
    existing_first_row = cursor.fetchone()

    if existing_first_row:
        existing_values = existing_first_row[1:]  #exclude id 
        new_values = tuple(new_first_row)

        # Compare first row data
        if existing_values == new_values:
            print("No change detected in first row. Skipping insertion.")
            return

    # If first row is different or no existing data, insert all new rows
    print("Change detected! Inserting new data...")
    
    for row in all_new_data:
        print("row about to be executed is:")
        print(row)
        cursor.execute("""
            INSERT INTO mktdata (Security, Trades, TTA, Open, High, Low, LTP, SIGNAL, T_G, LTY, Timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(row[:10]) + (current_timestamp,))  # ✅ CORRECT


    print("Data inserted successfully.")

# Insert new data into SQL
# Function to insert data into SQL
def insert_data_into_db(df):
    conn = connect_to_db() #establishes connection to the database.
    if conn is None:
        logging.error("Failed to establish database connection. Skipping data insert.")
        return
    print('Connected to the database.')
    cursor = conn.cursor()
    # Check if the DataFrame is empty
    if df.empty:
        logging.warning("No data to insert. DataFrame is empty.")
        conn.close()
        return
    # Extract the first row from the DataFrame
    new_first_row = df.iloc[0].tolist()  # Convert first row to a list
    # Convert entire DataFrame to a list of lists for batch insertion
    all_new_data = df.values.tolist()
    # Extract timestamp (assuming it's the same for all rows)
    current_timestamp = df.iloc[0]["Timestamp"]
    # Check and insert data based on the first row comparison
    check_and_insert_data(cursor, new_first_row, all_new_data, current_timestamp)
    # Commit and close connection
    conn.commit()
    conn.close()
    logging.info("New data inserted into SQL database.")


# Main Execution
if __name__ == "__main__":
    df = scrape_ccil()
    print(df)
    if df is not None:
        insert_data_into_db(df)
    else:
        logging.warning("No new data to insert.")
