"""API integration for syncing inventories from AlphaBroder to Shopify."""
import os
from api import API
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


if __name__ == '__main__':
    # TODO: Loops from Heroku DB
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    files = 'files'
    if not os.path.exists(files):
        os.mkdir(files)

    images = 'images'
    if not os.path.exists(images):
        os.mkdir(images)

    api = API(False, True)
    if os.environ["UPDATE"] == "products":
        limit = int(os.environ["LIMIT"])
        api.update_products(limit=limit)
        # api.update_inventory()
    else:
        api.update_inventory()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
