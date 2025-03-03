import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json

load_dotenv()

SEBRAE_HOST = os.getenv("SEBRAE_HOST")
DATALAKE_HOST = os.getenv("DATALAKE_HOST")
DATALAKE_DB = os.getenv("DATALAKE_DB")
DATALAKE_COLLECTION = os.getenv("DATALAKE_COLLECTION")

FONTE = 'files/fonte.json'
INSERT = 'files/insert.json'
INSERTED = 'files/inserted.json'
LINKS = 'files/links.txt'

def init():
    """Carrega o nome do banco de dados e a coleção de dados"""
    with open(FONTE, "r", encoding="utf-8") as file:
        data = json.load(file)
    
    return data["DB"] , data["COLLECTIONS"]

def get_collections(all_collections: list, research_names: list):
    """Retorna todas as coleções da pesquisa"""
    collections = []
    for name in all_collections:
        if any(part in name for part in research_names):
            collections.append(name)

    collections = verify_insert(collections)
    return collections

def get_link():
    """Obtém o primeiro link do arquivo link"""
    with open(LINKS, "r", encoding="utf-8") as file:
        first_link = file.readline().strip()
        return first_link if first_link else ''

def get_metadatas(db, collections):
    """Retorna os metadados que serão salvos no arquivo"""
    datas = {}
    for name in collections:
        c = db[name]
        doc = c.find_one({}, {'_id': False}) or {}
        data = {key: {'name': '', 'description': ''} for key in doc.keys()}
        datas.update(data)

    metadatas = {}
    metadatas['data'] = datas
    metadatas['collections'] = collections
    metadatas['link'] = get_link()
    return metadatas

def verify_metadata(datas):
    """Verifica se a chave ja foi preenchida"""
    with open(INSERTED, "r", encoding="utf-8") as file:
        data = json.load(file)

    if data:
        for key_inserted, value in data.items():
            if key_inserted in datas:
                datas[key_inserted] = value

    return datas

def verify_insert(collection_names):
    client = MongoClient(DATALAKE_HOST)
    collection = client[DATALAKE_DB][DATALAKE_COLLECTION]
    for doc in collection.find({}, {'collection': 1, '_id': 0}):
        collection_name = doc['collection']
        if any(name == collection_name for name in collection_names):
            print(f"{collection_name} já existe!")
            collection_names.remove(collection_name)

    client.close()
    return collection_names

def main():
    research_db, research_names = init()
    client = MongoClient(SEBRAE_HOST)
    db = client[research_db]
    collections = get_collections(db.list_collection_names(), research_names)
    metadatas = get_metadatas(db, collections)
    metadatas['data'] = verify_metadata(metadatas['data'])
    
    with open(INSERT, "w", encoding="utf-8") as file:
        json.dump(metadatas, file, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    main()