# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# import pandas as pd
# import time
# from datetime import datetime
# import pypyodbc
# import logging

# # Configure logging
# logging.basicConfig(filename="ccil_scraper.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # Connect to Azure SQL
# def connect_to_db():
#     try:
#         print('attempting to connect to database..')
#         conn = pypyodbc.connect(
#             "Driver={ODBC Driver 18 for SQL Server};Server=tcp:modular-server.database.windows.net,1433;Database=modular;Uid=CloudSA1c5b822c;Pwd={Givemeinternship!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
#         ) #connection string that was obtained from azure server.
#         print('returning database object...')
#         return conn
#     except pypyodbc.Error as e:
#         logging.error(f"Database connection failed: {e}")
#         return None

# #scraping ccil market data
# #selenium was used using a headless browser so that it can run on the virtual machine. beautiful soup cannot be used as the data from the table is loaded dynamically based on javascript.
# def scrape_ccil():
#     options = Options()
#     options.add_argument("--headless")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")

#     service = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=options)

#     url = "https://www.ccilindia.com/web/ccil/rbi-nds-om1"
#     driver.get(url)
#     time.sleep(5)
#     rows = driver.find_elements("xpath", "//table/tbody/tr") #extract all the relevant tables.
#     data = []
#     for row in rows:
#         res = []
#         cols = row.find_elements("tag name", "td")
#         if len(cols) < 10:  
#             continue
#         for col in cols:
#             res.append(col.text.strip())
#         data.append(res)

#     driver.quit()

#     if not data:
#         logging.warning("No data scraped from the website.")
#         return None

#     df = pd.DataFrame(data, columns=["Security", "Trades", "TTA", "Open", "High", "Low", "LTP", "Signal", "T/G", "LTY"])
#     df["Timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
#     return df #return the dataframe extracted from the most recent page.



# df = scrape_ccil()
