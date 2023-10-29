import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import glob

log_file = "log_file.txt"
target_file = "data.csv"

# Extract data from data-source

def extract_data_from_csv(file): # csv
  df = pd.read_csv(file)
  return df

def extract_data_from_json(file): # json
  df = pd.read_json(file, lines=True)
  return df

def extract_data_from_xml(file): # xml
  df = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

  tree = ET.parse(file)
  root = tree.getroot()

  for car in root:
    car_model = car.find("car_model").text
    year_of_manufacture = int(car.find("year_of_manufacture").text)
    price = float(car.find("price").text)
    fuel = car.find("fuel").text

    df2 = pd.DataFrame([{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])
    df = pd.concat([df, df2], ignore_index=True)
  return df

def extract():
  extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

  # extract data from csv
  for file in glob.glob("data-source/*.csv"):
    df = pd.DataFrame(extract_data_from_csv(file))
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)

  # extract data from json
  for file in glob.glob("data-source/*.json"):
    df = pd.DataFrame(extract_data_from_json(file))
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)

  # extract data from xml
  for file in glob.glob("data-source/*.xml"):
    df = pd.DataFrame(extract_data_from_xml(file))
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)
  return extracted_data

# Transform data

def transform(data):
  data["price"] = round(data.price, 2)
  return data

# Load data

def load_data(target_file, transformed_data):
  transformed_data.to_csv(target_file)

def logger(message):
  with open(log_file, "a") as f:
    f.write(str(datetime.now()) + " - " + message + "\n")


logger("ETL process Begin")
logger("--------------------")

logger("Data Extraction Begin")
extracted_data = extract()
logger("Data Extraction Complete")

logger("Data Transformation Begin")
transformed_data = transform(extracted_data)

print("transformed Data \n", transformed_data)
logger("Data Transformation Complete")

logger("Data Loading Begin")
load_data(target_file, transformed_data)
logger("Data Loading Complete")

logger("ETL process Complete")
logger("--------------------")