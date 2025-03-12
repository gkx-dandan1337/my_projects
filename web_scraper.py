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
    start_time = time.time()  # Record the start time
    retry_interval = 10  # Retry every 10 seconds
    max_retry_duration = 5 * 60  # Retry for a max of 5 minutes (300 seconds)
    while True:
        try:
            logging.info("Attempting to connect to the database...")
            conn = pypyodbc.connect(
                "Driver={ODBC Driver 18 for SQL Server};Server=tcp:modular-server.database.windows.net,1433;Database=modular;Uid=CloudSA1c5b822c;Pwd={Givemeinternship!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            logging.info("Returning database object...")
            return conn  # If successful, return the connection object

        except pypyodbc.Error as e:
            logging.error(f"Database connection failed: {e}")
            elapsed_time = time.time() - start_time  # Check the elapsed time
            if elapsed_time >= max_retry_duration:
                logging.error("Max retry duration reached. Exiting...")
                return None  # After 5 minutes, stop trying and return None
            else:
                logging.info(f"Retrying... Elapsed time: {elapsed_time:.2f} seconds")
                time.sleep(retry_interval)  # Wait for `retry_interval` seconds before retrying

#scraping ccil market data
#selenium was used using a headless browser so that it can run on the virtual machine. beautiful soup cannot be used as the data from the table is loaded dynamically based on javascript.
def scrape_ccil():
    logging.info(f'Scraping data...')
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

def row_exists(row, cursor):
    security = row["Security"]
    trades = row["Trades"]
    tta = row["TTA"]
    open = row["Open"]
    high = row["High"]
    low = row["Low"]
    ltp = row["LTP"]
    signal = row["Signal"]
    tg = row["T/G"]
    lty = row["LTY"]
    cursor.execute("""
    SELECT 1 FROM mktdata
    WHERE Security = ?
    AND Trades = ?
    AND TTA = ?
    AND [Open] = ?
    AND High = ?
    AND Low = ?
    AND LTP = ?
    AND SIGNAL = ?
    AND T_G = ?
    AND LTY = ?
    AND CONVERT(DATE, Timestamp) >= DATEADD(DAY, -1, CONVERT(DATE, GETDATE()))
    """, (security, trades, tta , open, high, low, ltp, signal, tg, lty))
    
    return cursor.fetchone() is not None #returns true if there are exact records found, otherwise returns false.


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
    rows_inserted = 0
    try:
        for index, row in df.iterrows():
            if row_exists(row, cursor):
                logging.info(f"Row already exists in the database: {row.to_dict()}")
                continue
            else:
                cursor.execute("""
                    INSERT INTO mktdata (Security, Trades, TTA, [Open], High, Low, LTP, SIGNAL, T_G, LTY, Timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (row["Security"], row["Trades"], row["TTA"], row["Open"], row["High"],
                      row["Low"], row["LTP"], row["Signal"], row["T/G"], row["LTY"], row["Timestamp"]))
                rows_inserted += 1

        if rows_inserted > 0:
            conn.commit()
            logging.info(f"Inserted {rows_inserted} new rows into SQL database.")

    except Exception as e:
        logging.error(f"Error while inserting data: {e}")
        conn.rollback()

    finally:
        conn.close()
        logging.info("Database connection closed.")

# Main Execution
def main():
    df = scrape_ccil()
    if df is not None:
        insert_data_into_db(df)
    else:
        logging.warning("No new data to insert.")

main() #call the function to run 