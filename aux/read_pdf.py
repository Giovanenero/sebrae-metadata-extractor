import os
import json
import tabula
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import re


FILE = 'ddi-documentation-portuguese-70.pdf'
PAGES = [4,5]

PATH = '/home/giovane/Downloads/'
JSON_FILE = './../files/inserted.json'
FONTE = './../files/fonte.json'

load_dotenv()
DATALAKE_HOST = os.getenv("DATALAKE_HOST")
DATALAKE_DB = os.getenv("DATALAKE_DB")
DATALAKE_COLLECTION = os.getenv("DATALAKE_COLLECTION")

file_path = PATH + FILE
tables = tabula.read_pdf(file_path, pages=PAGES)

os.system('clear')
print(f'Tabelas extraídas: {len(tables)}')

with open(JSON_FILE, "r", encoding="utf-8") as file:
    metadatas = json.load(file)

with open(FONTE, "r", encoding="utf-8") as file:
    data = json.load(file)

db_name = data.get('DB')

client = MongoClient(DATALAKE_HOST)
collection = client[DATALAKE_DB][DATALAKE_COLLECTION]

print(tables[0].columns)

column_code = 'Código'
column_description = 'Nome'

columns_to_find = {row[table.columns.get_loc(column_code)] for table in tables for row in table.itertuples(index=False)}
query_results = {doc["column"]: doc["key"] for doc in collection.find({"db": db_name, "column": {"$in": list(columns_to_find)}})}

new_data = {}
last_valid_values = {}

for table in tables:
    for row in table.itertuples(index=False):
        description = str(row[table.columns.get_loc(column_description)]).strip()
        code = row[table.columns.get_loc(column_code)]

        #code = re.sub(r'([a-z])([A-Z])', r'\1_\2', str(code)).upper()

        if not pd.notna(code):
            if last_valid_values.get('Código'):
                column = last_valid_values['Código']
                new_data[column]['description'] += ' ' + description
                last_valid_values["Nome"] = new_data[column]['description']
            continue

        column = str(code).strip()

        last_valid_values["Código"] = column
        last_valid_values["Nome"] = description

        name = query_results.get(column, "")

        new_data[column] = {
            "name": name,
            "description": description
        }

client.close()
metadatas.update(new_data)

with open(JSON_FILE, "w", encoding="utf-8") as file:
    json.dump(metadatas, file, ensure_ascii=False, indent=4)

print(f"Metadados atualizados e salvos em {JSON_FILE}")