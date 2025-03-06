import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import logging
import unicodedata
import re

logging.basicConfig(
    filename="extractor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S"
)

load_dotenv()

DATALAKE_HOST = os.getenv("DATALAKE_HOST")
DATALAKE_DB = os.getenv("DATALAKE_DB")
DATALAKE_COLLECTION = os.getenv("DATALAKE_COLLECTION")

SEBRAE_HOST = os.getenv("SEBRAE_HOST")

FONTE = 'files/fonte.json'
LINKS = 'files/links.txt'
INSERT = 'files/insert.json'

def check_empty_fields(datas):
    empty_names = []
    empty_descriptions = []
    for key, value in datas.items():
        if value['name'] == '':
            empty_names.append(key)
        if value['description'] == '':
            empty_descriptions.append(key)


    if empty_names or empty_descriptions:
        print('CAMPOS VAZIOS:\n')
        if empty_names:
            print('====================== names ======================\n')
            for key in empty_names:
                print(key)

        if empty_descriptions:
            print('====================== descriptions ======================\n')
            for key in empty_descriptions:
                print(key)
            
        print('\nAdicionar mesmo assim?[y/n]')
        add = input().strip().lower() 
        if add not in ('y', 'yes'):
            print('Encerrando programa!')
            exit(0)

def get_db_name():
    """Carrega o nome do banco de dados"""
    with open(FONTE, "r", encoding="utf-8") as file:
        data = json.load(file)
    
    return data["DB"]

def get_collections_name():
    """Carrega o nome das coleções da extração"""
    with open(INSERT, "r", encoding="utf-8") as file:
        data = json.load(file)

    return data['collections']

def get_type(collection, column):
    """Retorna o tipo do primeiro valor não nulo encontrado em uma coluna da coleção MongoDB"""
    doc = collection.find_one({column: {"$ne": None}}, {column: 1, '_id': 0})  # Busca direto um valor não nulo

    return type(doc[column]).__name__ if doc else type(None).__name__

def get_datas():
    with open(INSERT, "r", encoding="utf-8") as file:
        data = json.load(file)

    return data

def get_text(text):
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[^a-z0-9 _]', '', text.lower())
    return text

def get_metadata(data, collection, c_name, db_name):
    metadatas = []
    doc = collection.find_one({}, {'_id': False})
    default_key = 'sem_chave'
    default_description = 'Sem descrição'
    for key, value in doc.items():
        type_name = type(value).__name__ if value is not None else get_type(collection, key)
        name = data[key]['name']
        description = data[key]['description']
        metadatas.append({
            'db': db_name,
            'collection': c_name,
            'column': key,
            'type': type_name,
            'key': get_text(name) if name != '' else default_key,
            'description': description if name != '' else default_description,
        })
    return metadatas

def remove_link():
    with open(LINKS, "r") as f:
        lines = f.readlines()

    if lines:
        with open(LINKS, "w") as f:
            f.writelines(lines[1:])

def verify_insert(c_datalake, name):
    exists = c_datalake.count_documents({"collection": name}, limit=1) > 0
    if exists:
        print(f"{name} já existe!")

    return exists

def main():
    data = get_datas()
    link = data['link']
    data = data['data']
    check_empty_fields(data)
    db_name = get_db_name()
    collections_name = get_collections_name()

    client_sebrae = MongoClient(SEBRAE_HOST)
    db_sebrae = client_sebrae[db_name]

    client_datalake = MongoClient(DATALAKE_HOST)
    c_datalake = client_datalake[DATALAKE_DB][DATALAKE_COLLECTION]

    print('===================== Adicionando =====================\n')
    for name in collections_name:
        if verify_insert(c_datalake, name):
            continue
        metadata = get_metadata(data, db_sebrae[name], name, db_name)
        c_datalake.insert_many(metadata)
        log = f"Adicionando: {db_name} - {name}"
        if link != "":
            log += f" - link: {link}"
        logging.info(log)
        print(name)

    client_sebrae.close()
    client_datalake.close()
    remove_link()

if __name__ == '__main__':
    main()
