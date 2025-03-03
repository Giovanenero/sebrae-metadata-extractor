import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import logging

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

FONTE = 'fonte.json'
LINKS = 'links.txt'
INSERT = 'insert.json'

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
    docs = collection.find({}, {column: 1, '_id': 0})  

    for doc in docs:
        value = doc.get(column)
        if value is not None: return type(value).__name__
    return type(None).__name__

def get_datas():
    with open(INSERT, "r", encoding="utf-8") as file:
        data = json.load(file)

    return data

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
            'key': name if name != '' else default_key,
            'description': description if name != '' else default_description,
        })
    return metadatas

def insert(metadata):
    client = MongoClient(DATALAKE_HOST)
    #client = MongoClient('mongodb://localhost:27017')
    collection = client[DATALAKE_DB][DATALAKE_COLLECTION]
    collection.insert_many(metadata)
    client.close()

def remove_link():
    with open(LINKS, "r") as f:
        lines = f.readlines()

    if lines:
        with open(LINKS, "w") as f:
            f.writelines(lines[1:])

def main():
    data = get_datas()
    link = data['link']
    data = data['data']
    check_empty_fields(data)
    db_name = get_db_name()
    collections_name = get_collections_name()

    client = MongoClient(SEBRAE_HOST)
    db = client[db_name]
    print('===================== Adicionando =====================\n')
    for name in collections_name:
        metadata = get_metadata(data, db[name], name, db_name)
        insert(metadata)
        log = f"Adicionando: {db_name} - {name}"
        if link != "":
            log += f" - link: {link}"
        logging.info(log)
        print(name)

    client.close()
    remove_link()

if __name__ == '__main__':
    main()
