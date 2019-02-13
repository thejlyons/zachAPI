"""API Class for product and inventory management."""
import os
import pymongo
import shopify
from ftplib import FTP_TLS
import urllib.request
import pandas as pd
from datetime import datetime


class API:
    """
    Class for handling and managing all API inventory items.

    _inventory = {<Item Number>: {
        "Catalog Page Number": ...,
        "NEW": ...,
        "Item Number": ...,
        "Style": ...,
        "Short Description": ...,
        "Color Code": ...,
        "Size Code": ...,
        "Case Qty": ...,
        "Weight": ...,
        "Mill #": ...,
        "Mill Name": ...,
        "Category": ...,
        "Subcategory": ...,
        "Thumbnail Name": ...,
        "Normal Image Name": ...,
        "Full Feature Description": ...,
        "Brand Page Number": ...,
        "Front of Image Name": ...,
        "Back of Image Name": ...,
        "Side of Image Name": ...,
        "Gtin": ...,
        "Launch Date": ...,
        "PMS Color": ...,
        "Size Sort Order": ...
    }}

    _colors = {<Color Code>: {
        "Color Name": ...,
        "Color Group Code": ...,
        "Hex Code": ...
    }}

    _sizes = {<Size Code>: {
        "Size Group": ...,
        "Size": ...
    }}

    _shopify_ids = {<Item Number>: Variant.id}
    """
    _inventory = {}
    _prices = None
    _colors = {}
    _sizes = {}
    _images = {}
    _shopify_ids = {}
    _color_groups = ["", "Basic Colors", "Traditional Colors", "Extended Colors", "Extended Colors 2",
                     "Extended Colors 3", "Extended Colors 4", "Extended Colors 5", "Extended Colors 6",
                     "Extended Colors 7", "Extended Colors 8", "Extended Colors 9", "Extended Colors 10"]
    _image_url = "https://www.alphabroder.com/images/alp/prodDetail/{}".format
    _product_file = 'AllDBInfoALP_Prod.txt'
    _price_file = 'AllDBInfoALP_PRC_R034.txt'
    _inventory_file = 'inventory-v8-alp.txt'
    _categories = ['Polos', 'Outerwear', 'Fleece', 'Sweatshirts', 'Woven Shirts', 'T-Shirts', 'Infants | Toddlers']

    def __init__(self, download=True, debug=False):
        """Initialize inventory by parsing provided inventory CSV file and building a dict of all inventory items."""
        self._db = None
        self._download = download
        self._debug = debug
        self._current_products = {}
        self._current_variants = {}

    def update_inventory(self):
        """Update all product inventory values."""
        self._current_products = {}
        if self._download:
            self.debug("Downloading files.")
            self.prepare_inventory()

        self.debug("Connecting to shopify.")
        shop_url = "https://{}:{}@{}.myshopify.com/admin".format(os.environ["SHOPIFY_API_KEY"],
                                                                 os.environ["SHOPIFY_PASSWORD"],
                                                                 os.environ["SHOPIFY_STORE"])
        shopify.ShopifyResource.set_site(shop_url)

        if not self._db:
            self._db = self.init_mongodb()

        inventory = self._db.inventory.find()
        try:
            inventory = [item for item in inventory][0]
        except IndexError:
            inventory = []
        inventory = {key: inventory[key] for key in inventory if key != '_id'}

        # Parse Inventory File
        self.debug("Parsing Inventory File")
        df = pd.read_csv(os.path.join('files', self._inventory_file), delimiter=',', engine='python')

        total = len(inventory.keys())
        progress = []
        for i, (item_number, item) in enumerate(inventory.items()):
            row = df.loc[df['Item Number'] == item_number]
            if not row.empty:
                if item["product_id"] not in self._current_products:
                    self._current_products[item["product_id"]] = shopify.Product.find(item["product_id"])

                for x in range(len(self._current_products[item["product_id"]].variants)):
                    if item["variant_id"] == self._current_products[item["product_id"]].variants[x].id:
                        self._current_products[item["product_id"]].variants[x].inventory_quantity = int(
                            row["Total Inventory"].values[0])

            p = int(100 * i / total)
            if p % 5 == 0 and p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)
        self.debug("100%\n")

        total = len(self._current_products.keys())
        progress = []
        for i, (pid, product) in enumerate(self._current_products.items()):
            product.save()

            p = int(100 * i / total)
            if p % 5 == 0 and p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)

        self.debug("100%\n")
        self._clean()

    def update_products(self, limit=False):
        """Update all products."""
        if self._download:
            self.debug("Downloading files.")
            self.prepare_products()

        self.debug("Connecting to shopify.")
        shop_url = "https://{}:{}@{}.myshopify.com/admin".format(os.environ["SHOPIFY_API_KEY"],
                                                                 os.environ["SHOPIFY_PASSWORD"],
                                                                 os.environ["SHOPIFY_STORE"])
        shopify.ShopifyResource.set_site(shop_url)

        self.debug("Retrieving database entries.")
        # changes = self.find_changes()

        if not self._db:
            self._db = self.init_mongodb()
        inventory_store = self._db.inventory.find()
        try:
            inventory_store = [item for item in inventory_store][0]
        except IndexError:
            inventory_store = []
        inventory_store = [key for key in inventory_store if key != '_id']

        # Parse Product File
        self.debug("Parsing Product File")

        self._inventory = pd.read_csv(os.path.join('files', self._product_file), delimiter='^', engine='python')
        self._inventory = self._inventory.loc[self._inventory['Category'].isin(self._categories)
                                              & ~self._inventory['Item Number'].isin(inventory_store)]
        #                                     & (self._inventory['Style'] == 'TT11YL')]
        self._inventory = self._inventory.replace({"Mill Name": {'Bella + Canvas': 'Bella+Canvas'}})

        self._inventory.sort_values('Style')

        if limit:
            self._inventory = self._inventory.head(5000)

        self.debug("Processing products.")
        total = self._inventory.shape[0]
        progress = []
        for i, (index, item) in enumerate(self._inventory.iterrows()):
            if item["Style"] not in self._current_products:
                products = shopify.Product.find(limit=250, vendor=item["Mill Name"])
                products = [p for p in products if item["Style"] in p.title]
                self._current_products[item["Style"]] = products
            of_color = self._inventory.loc[(self._inventory["Style"] == item["Style"])
                                           & (self._inventory["Color Name"] == item["Color Name"])].shape[0]
            self.process_item(item, of_color)
            p = int(100 * i / total)
            if p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)
        self.debug("100%\n")

        self.debug("Saving new products.")
        self.save_new_products()

        self.debug("Updating Database.")
        self._db.inventory.delete_many({})
        self._db.inventory.insert_one(self._shopify_ids)
        self._clean()

    def process_item(self, item, of_color):
        """Check if item already exists. If not, create a new variant and add it."""

        """Update item in the Shopify store."""
        skip = False
        product = self._current_products[item["Style"]][-1] if len(self._current_products[item["Style"]]) > 0 else None
        similar_variants = 0
        for p in self._current_products[item["Style"]]:
            color_option = ""
            color = item["Color Name"].lower()
            size_option = ""
            size = item["Size"].lower()

            for option in p.options:
                if color in [o.lower() for o in option.values]:
                    color_option = 'option{}'.format(option.position)
                elif size in [o.lower() for o in option.values]:
                    size_option = 'option{}'.format(option.position)

            if color_option and size_option:
                product = p
                variant = None
                for v in p.variants:
                    if v.attributes[color_option].lower() == color \
                            and v.attributes[size_option].lower() == size:
                        variant = v
                if variant:
                    self._shopify_ids[item["Item Number"]] = {
                        "variant_id": variant.id,
                        "product_id": product.id
                    }
                    skip = True
                    break

        if not skip:
            if not product:
                product = self.new_product(item["Mill Name"], item["Style"], item["Short Description"],
                                           item["Category"], len(self._current_products[item["Style"]]))

            color_option = ""
            size_option = ""
            try:
                color_option = ['option{}'.format(o.position) for o in product.options if o.name.lower() == "color"][0]
                size_option = ['option{}'.format(o.position) for o in product.options if o.name.lower() == "size"][0]
            except IndexError:
                size_option = 'option1'
                color_option = 'option2'

            # If total variants, plus the all of that color, mines the ones already made with the same color >= 100
            # then a new product needs created. Shopify limits 100 variants / product.
            color = item["Color Name"].lower()
            similar_variants = len([v for v in product.variants if v.attributes[color_option].lower() == color])
            if len(product.variants) + of_color - similar_variants >= 100:
                # product.options = [{"name": "Color", "values": [v[color_option] for v in product.variants]},
                #                    {"name": "Size", "values": [v[size_option] for v in product.variants]}]
                for x in range(len(self._current_products[item["Style"]])):
                    if product.id == self._current_products[item["Style"]][x].id:
                        self._current_products[item["Style"]][x] = product
                        break

                size_option = 'option1'
                color_option = 'option2'
                product = self.new_product(item["Mill Name"], item["Style"], item["Short Description"],
                                           item["Category"], len(self._current_products[item["Style"]]))

            filename = item["Front of Image Name"]
            image = self.find_image(filename, product.id)
            image_id = None
            if image:
                product.images.append(image)
                image_id = image.id

            if self._prices is None:
                self._prices = pd.read_csv(os.path.join('files', self._price_file), delimiter='^', engine='python')

            price = self._prices.loc[self._prices["Item Number "] == item["Item Number"]]
            if not price.empty:
                price = price["Piece"].values[0]
            else:
                price = 0
            variant = shopify.Variant({color_option: item["Color Name"].title(), size_option: item["Size"],
                                       'price': price, 'image_id': image_id, 'product_id': product.id})
            self._current_variants.setdefault(item["Style"], []).append(variant)
            product.variants.append(variant)

            found = False
            for x in range(len(self._current_products[item["Style"]])):
                if product.id == self._current_products[item["Style"]][x].id:
                    self._current_products[item["Style"]][x] = product
                    found = True
                    break
            if not found:
                self._current_products[item["Style"]].append(product)

    def save_new_products(self):
        """Loop through self._current_products and save the last in each list"""
        total = len(self._current_products.keys())
        progress = []
        for i, (key, products) in enumerate(self._current_products.items()):
            p = int(100 * i / total)
            if p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)

            main_product = ""
            for product in products:
                metafields = product.metafields()
                for mf in metafields:
                    if mf.namespace == "api_integration" and mf.key == "main_product":
                        main_product = mf.value
            if not main_product:
                main_product = products[0].handle
            other_products = [product.handle for product in products if product.handle != main_product]
            for product in products:
                color_option = ""
                size_option = ""
                try:
                    color_option = ['option{}'.format(o.position) for o in product.options if o.name.lower() == "color"]
                    color_option = color_option[0]
                    size_option = ['option{}'.format(o.position) for o in product.options if o.name.lower() == "size"]
                    size_option = size_option[0]
                except IndexError:
                    size_option = 'option1'
                    color_option = 'option2'
                product.options = [{"name": "Size", "values": [v.attributes[size_option] for v in product.variants]},
                                   {"name": "Color", "values": [v.attributes[color_option] for v in product.variants]}]

                metafields = product.metafields()
                mp = False
                op = False
                for mf in metafields:
                    if mf.namespace == 'api_integration':
                        if mf.key == 'main_product':
                            mp = True
                            mf.value = main_product
                        elif mf.key == 'other_product':
                            op = True
                            mf.value = other_products
                        mf.save()

                if not mp:
                    product.add_metafield(shopify.Metafield({
                        'key': 'main_product',
                        'value': main_product,
                        'value_type': 'string',
                        'namespace': 'api_integration'
                    }))
                if not op:
                    product.add_metafield(shopify.Metafield({
                        'key': 'other_products',
                        'value': ",".join(other_products),
                        'value_type': 'string',
                        'namespace': 'api_integration'
                    }))

                product.save()

                if key in self._current_variants:
                    for v1 in product.variants:
                        for v2 in self._current_variants[key]:
                            if v1.attributes[color_option].lower() == v2.attributes[color_option].lower() \
                                    and v1.attributes[size_option].lower() == v2.attributes[size_option].lower():
                                v1.image_id = v2.image_id
                                v1.save()
                                row = self._inventory.loc[
                                    (self._inventory["Style"] == key)
                                    & (self._inventory["Color Name"] == v1.attributes[color_option].upper())
                                    & (self._inventory["Size"] == v1.attributes[size_option])
                                ]
                                if not row.empty:
                                    self._shopify_ids[row["Item Number"].values[0]] = {
                                        "variant_id": v1.id,
                                        "product_id": product.id
                                    }
        self.debug("100%\n")

    def new_product(self, mill_name, style, short_description, category, color_index):
        """Create a new Shopify product with the given data."""
        new_product = shopify.Product()
        title = "{} {}: {}".format(mill_name, style, short_description)
        if color_index > 0:
            title = "{}, {}".format(title, self._color_groups[color_index])
        new_product.title = title
        new_product.vendor = mill_name
        new_product.product_type = category
        new_product.save()
        new_product.variants = []
        return new_product

    def prepare_products(self):
        """Prepare for updating products by downloading relevant files."""
        self.download(self._product_file)
        self.download(self._price_file)

    def prepare_inventory(self):
        """Prepare for updating product inventory by downloading relevant files."""
        self.download(self._inventory_file)

    def download(self, filename):
        """Download the given file."""
        ftp = FTP_TLS(os.environ["FTP_DOMAIN"])
        ftp.login(user=os.environ["FTP_USERNAME"], passwd=os.environ["FTP_PASSWORD"])

        self.download_file(ftp, filename)
        self.download_file(ftp, filename)

        ftp.quit()

    def find_image(self, filename, product_id):
        """Check if image in self._images otherwise download and create it."""
        if filename in self._images and self._images[filename].product_id == product_id:
            return self._images[filename]
        else:
            if isinstance(filename, str):
                url = self._image_url(filename)
                # print(url)

                file_location = os.path.join('images', filename)

                opener = urllib.request.build_opener()
                opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                                    '(KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36')]
                urllib.request.install_opener(opener)
                urllib.request.urlretrieve(url, file_location)

                image = shopify.Image({"product_id": product_id})

                with open(file_location, 'rb') as f:
                    encoded = f.read()
                    image.attach_image(encoded, file_location)
                    image.save()
                self._images[filename] = image
                return image
            else:
                return None

    def _clean(self):
        """Remove all downloaded files."""
        folder = 'images'
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

        if self._download:
            if os.path.isfile(os.path.join('files', self._inventory_file)):
                os.unlink(os.path.join('files', self._inventory_file))

            if os.path.isfile(os.path.join('files', self._product_file)):
                os.unlink(os.path.join('files', self._product_file))

            if os.path.isfile(os.path.join('files', self._price_file)):
                os.unlink(os.path.join('files', self._price_file))

    @staticmethod
    def init_mongodb():
        """Initialize MongoDB Client."""
        client = pymongo.MongoClient(os.environ["MONGODB_URL"])
        return client.bulkthreads

    def download_file(self, ftp, filename):
        """Download given file from global FTP server."""

        download_to = os.path.join('files', filename)
        self.debug("Downloading '{}' to: {}".format(filename, download_to))

        local_file = open(download_to, 'wb')
        ftp.retrbinary('RETR ' + filename, local_file.write)
        local_file.close()

    def debug(self, msg):
        """Method for printing debug messages."""
        if self._debug:
            print("<{}>: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
