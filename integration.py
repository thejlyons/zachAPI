"""API integration for syncing inventories from AlphaBroder to Shopify."""
from api import API
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


if __name__ == '__main__':
    # TODO: Loops from Heroku DB
    # TODO: Create files/ and images/ if they don't exist
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    api = API(True, True)
    api.update_products()
    api.update_inventory()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
