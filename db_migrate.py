import pymongo
from dotenv import load_dotenv
import os
import json

load_dotenv()


def sanitize_records(records):
    """Return records without the default mongodb _id to avoid conflicts on save"""
    try:
        records = [item for item in records][0]
    except IndexError:
        records = []
    records = {key: records[key] for key in records if key != '_id'}
    return records


client = pymongo.MongoClient(os.environ["MONGODB_URL"])
db = client.bulkthreads

products = sanitize_records(db.products.find())
with open('/Users/jlyons/Github/zachAPI/backups/new.json', 'w', encoding='utf-8') as f:
    json.dump(products, f, ensure_ascii=False, indent=4)

inventory = sanitize_records(db.inventory.find())
with open('/Users/jlyons/Github/zachAPI/backups/inventory_20200914.json', 'w', encoding='utf-8') as f:
    json.dump(inventory, f, ensure_ascii=False, indent=4)
