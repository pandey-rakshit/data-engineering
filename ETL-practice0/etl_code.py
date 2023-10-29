# Imports All required packages

import glob
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd


log_file = "log_file.txt"
target_file = "data.csv"

# Extract data from data-source

def extract_data_from_csv(file):
  df = pd.read_csv(file)
  return df

def extract_data_from_json(file):
  df = pd.read_json(file, lines=True)
  return df

def extract_data_from_xml(file):

  df = pd.DataFrame(columns=["name", "height", "weight"])
  tree = ET.parse(file)
  root = tree.getroot()
  for person in root:
    name = person.find("name").text
    height = float(person.find("height").text)
    weight = float(person.find("weight").text)
    df2 = pd.DataFrame([{"name": name, "height": height, "weight": weight}])
    df = pd.concat([df, df2], ignore_index=True)
  return df

def extract():
  extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

  # extract data from csv
  for file in glob.glob("data-source/*.csv"):
    df = extract_data_from_csv(file)
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)

  # extract data from json
  for file in glob.glob("data-source/*.json"):
    df = extract_data_from_json(file)
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)

  # extract data from xml
  for file in glob.glob("data-source/*.xml"):
    df = extract_data_from_xml(file)
    extracted_data = pd.concat([extracted_data, df], ignore_index=True)

  return extracted_data

# Transform data 

def transform(data):
  data['height'] = round(data.height * 0.0254, 2)
  data['weight'] = round(data.weight * 0.45359237, 2)

  return data

# Load data

def load_data(target_file, transformed_data):
  transformed_data.to_csv(target_file)

# logging

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
print("transformed data \n", transformed_data)
logger("Data Transformation Complete")

logger("Data Loading Begin")
load_data(target_file, transformed_data)
logger("Data Loading Complete")

logger("ETL process Complete")
logger("--------------------")