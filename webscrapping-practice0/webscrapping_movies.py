from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

log_file = "log_file.txt"

def extract_data(url):
  page = requests.get(url).text
  data = BeautifulSoup(page, 'html.parser')
  df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
  tables = data.find_all("tbody")
  rows = tables[0].find_all("tr")
  count = 0
  for row in rows:
    if count < 50:
      cols = row.find_all("td")
      if len(cols) != 0:
        rank = cols[0].text
        film = cols[1].text
        year = cols[2].text

        df2 = pd.DataFrame([{"Average Rank": rank, "Film": film, "Year": year}])
        df = pd.concat([df, df2], ignore_index=True)
        count += 1
    else:
       break
    
  return df
    
def load_data(target_file, data):
  data.to_csv(target_file)

def save_in_database(db_name, table_name, data):
  conn = sqlite3.connect(db_name)
  data.to_sql(table_name, conn, if_exists='replace', index=False)
  conn.close()

def logger(message):
  with open(log_file, "a") as f:
    f.write(str(datetime.now()) + " - " + message + "\n")


logger("Webscrapping Practice Demo")
logger("-----------------------------")

logger("Data Extraction Begin")
extracted_data = extract_data(url)
print("extracted_data \n", extracted_data)
logger("Data Extraction Completed")

logger("Data loading")
load_data(csv_path, extracted_data)
logger("Data loading Completed")

logger("Data Saving")
save_in_database(db_name, table_name, extracted_data)
logger("Data Saving Completed")
logger("----------------------------")