import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
SEBRAE_HOST = os.getenv("SEBRAE_HOST")

SEARCH = [
    'IN_RELICABILIDADE'
]


def get_collections(all_collections: list, research_names: list):
    """Retorna todas as coleções da pesquisa"""
    collections = []
    for name in all_collections:
        if any(part in name for part in research_names):
            collections.append(name)

    return collections

db = MongoClient(SEBRAE_HOST)['CAPES']
collections = get_collections(db.list_collection_names(), ['PROD'])

for collection in collections:
    doc = db[collection].find_one()
    keys = doc.keys()
    if any(key in SEARCH for key in keys):
        print(collection)
        print()
