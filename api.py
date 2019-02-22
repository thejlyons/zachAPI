"""API Class for product and inventory management."""
import os
import re
import pymongo
import shopify
from ftplib import FTP_TLS
import urllib.request
from urllib.error import HTTPError
from pyactiveresource.connection import ResourceNotFound, ServerError
import pandas as pd
from datetime import datetime
from time import sleep

from threading import Thread
import shopify_limits


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
    _image_url = "https://www.alphabroder.com/media/hires/{}".format
    _product_file = 'AllDBInfoALP_Prod.txt'
    _price_file = 'AllDBInfoALP_PRC_R034.txt'
    _inventory_file = 'inventory-v8-alp.txt'
    _categories = ['T-Shirts']
    # _categories = ['Polos', 'Outerwear', 'Fleece', 'Sweatshirts', 'Woven Shirts', 'T-Shirts', 'Infants | Toddlers']

    def __init__(self, download=True, debug=False):
        """Initialize inventory by parsing provided inventory CSV file and building a dict of all inventory items."""
        self._db = None
        self._download = download
        self._debug = debug
        self._current_products = {}
        self._product_images = {}
        self._styles_to_fix = []
        self._categories = os.environ['CATEGORIES'].split(",")

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

        i = 0
        self.debug("Getting page {}".format(i))
        products = shopify.Product.find(limit=250, page=i)
        while len(products) > 0:
            all_products = {}
            self._current_products = {}
            for product in products:
                all_products[product.id] = product
            i += 1
            self.debug("Getting page {}".format(i))
            products = shopify.Product.find(limit=250, page=i)

            total = len(inventory.keys())
            progress = []
            for i, (item_number, item) in enumerate(inventory.items()):
                row = df.loc[df['Item Number'] == item_number]
                if not row.empty:
                    if item["product_id"] not in self._current_products:
                        try:
                            self._current_products[item["product_id"]] = all_products[item["product_id"]]
                        except KeyError:
                            pass

                    if item["product_id"] in self._current_products:
                        for x in range(len(self._current_products[item["product_id"]].variants)):
                            if item["variant_id"] == self._current_products[item["product_id"]].variants[x].id:
                                self._current_products[item["product_id"]].variants[x].inventory_quantity = int(
                                    row["Total Inventory"].values[0])

                p = int(100 * i / total)
                if p % 5 == 0 and p not in progress:
                    self.debug("{}%".format(p))
                    progress.append(p)
            self.debug("100%\n")

            for x, products in enumerate(list(self.chunks([i for k, i in self._current_products.items()],
                                                          int(len(self._current_products.keys()) / 10)))):
                t = Thread(target=self.save_thread, args=(x, products,))
                t.start()
            # total = len(self._current_products.keys())
            # progress = []
            # for i, (pid, product) in enumerate(self._current_products.items()):
            #     sleep(0.5)
            #     product.save()
            #
            #     p = int(100 * i / total)
            #     if p % 5 == 0 and p not in progress:
            #         self.debug("{}%".format(p))
            #         progress.append(p)

        self.debug("100%\n")
        self._clean()

    @staticmethod
    def save_thread(thread_number, products):
        """Thread for saving a list of products."""
        total = len(products)
        progress = []
        for i, product in enumerate(products):
            product.save()
            p = int(100 * i / total)
            if p % 5 == 0 and p not in progress:
                print("<{}>: Thread {} is {}% finished".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                               thread_number, p))
                progress.append(p)

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
        inventory_store = {key: inventory_store[key] for key in inventory_store if key != '_id'}

        # Parse Product File
        self.debug("Parsing Product File")

        self._inventory = pd.read_csv(os.path.join('files', self._product_file), delimiter='^', engine='python')
        self._inventory = self._inventory.loc[self._inventory['Category'].isin(self._categories)
                                              & ~self._inventory['Mill Name'].str.contains('Drop Ship',
                                                                                           flags=re.IGNORECASE,
                                                                                           regex=True)]
        if os.environ['SKIP_EXISTING'] == "True":
            self._inventory = self._inventory.loc[~self._inventory['Item Number'].isin(inventory_store.keys())]
        if os.environ['ONLY_THESE']:
            these = os.environ['ONLY_THESE'].split(",")
            self._inventory = self._inventory.loc[self._inventory['Style'].isin(these)]
        self._inventory = self._inventory.replace({"Mill Name": {'Bella + Canvas': 'Bella+Canvas'}})

        self._inventory.sort_values('Style')

        self.debug(self._inventory.shape[0])
        if limit:
            self._inventory = self._inventory.head(10000)

        self.debug("Processing products.")
        total = self._inventory.shape[0]
        self.debug(total)
        progress = []
        for i, (index, item) in enumerate(self._inventory.iterrows()):
            if 'Drop Ship' in item["Mill Name"]:
                continue
            if item["Style"] not in self._current_products:
                sleep(0.5)
                all_products = shopify.Product.find(limit=250, vendor=item["Mill Name"])
                products = []
                for product in all_products:
                    if item["Style"] in product.title:
                        style = product.title.split(':')
                        style = style[0].split(" ")[-1]
                        if item["Style"] == style:
                            if len(product.options) > 0 and product.options[0].name == 'Color':
                                print("----{} needs fixed.----".format(item["Style"]))
                                self._styles_to_fix.append(item["Style"])
                                sleep(0.5)
                                product = shopify.Product.find(product.id)
                            if len(product.variants) == 1 and product.variants[0].title == 'Default Title':
                                product.variants = []
                            products.append(product)
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
        for key, item in inventory_store.items():
            self._shopify_ids[key] = item
        self._db.inventory.delete_many({})
        self._db.inventory.insert_one(self._shopify_ids)
        self._clean()

        print(", ".join(self._styles_to_fix))

    def process_item(self, item, of_color):
        """Check if item already exists. If not, create a new variant and add it."""

        """Update item in the Shopify store."""
        skip = False

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
                variant = None
                for v in p.variants:
                    if v.attributes[color_option].lower() == color \
                            and v.attributes[size_option].lower() == size:
                        variant = v
                if variant:
                    self._shopify_ids[item["Item Number"]] = {
                        "variant_id": variant.id,
                        "product_id": p.id
                    }
                    skip = True
                    break

        product = None
        for p in self._current_products[item["Style"]]:
            if len(p.variants) + of_color - similar_variants < 100:
                product = p
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
            similar_variants = len([v for v in product.variants if str(v.attributes[color_option]).lower() == color])
            if len(product.variants) + of_color - similar_variants >= 100:
                for x in range(len(self._current_products[item["Style"]])):
                    if product.id == self._current_products[item["Style"]][x].id:
                        self._current_products[item["Style"]][x] = product
                        break

                size_option = 'option1'
                color_option = 'option2'
                product = self.new_product(item["Mill Name"], item["Style"], item["Short Description"],
                                           item["Category"], len(self._current_products[item["Style"]]))

            if self._prices is None:
                self._prices = pd.read_csv(os.path.join('files', self._price_file), delimiter='^', engine='python')

            price = self._prices.loc[self._prices["Item Number "] == item["Item Number"]]
            if not price.empty:
                price = price["Piece"].values[0]
                try:
                    int(price)
                except ValueError:
                    price = 0
            else:
                price = 0
            variant = shopify.Variant({color_option: item["Color Name"].title(), size_option: item["Size"],
                                       'product_id': product.id})
            if price != 0 and price != "":
                variant.price = price
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
        total = 0
        for k, products in self._current_products.items():
            total += sum([len(p.variants) for p in products])
        i = 0
        progress = []
        for key, products in self._current_products.items():
            main_product = ""
            if len(products) > 4:
                print("----Check {}----".format(key))
            for product in products:
                if product.body_html:
                    main_product = product.handle
                    break
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
                        sleep(0.5)
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

                try:
                    sleep(0.5)
                    product.save()
                except HTTPError:
                    pass
                except ServerError:
                    try:
                        sleep(60)
                        product.save()
                    except HTTPError:
                        print("----Could not save product({})----".format(product.id))
                        continue
                    except ServerError:
                        print("----Could not save product({})----".format(product.id))
                        continue

                for variant in product.variants:
                    row = self._inventory.loc[
                        (self._inventory["Style"] == key)
                        & (self._inventory["Color Name"] == str(variant.attributes[color_option]).upper())
                        & (self._inventory["Size"] == variant.attributes[size_option])
                    ]
                    if not row.empty and row["Item Number"].values[0] not in self._shopify_ids:
                        filename = row["Front of Image Name"].values[0]
                        image = self.find_image(filename, product.id)
                        if image:
                            variant.image_id = image.id
                            try:
                                sleep(0.5)
                                variant.save()
                            except HTTPError:
                                pass
                            except ServerError:
                                try:
                                    sleep(60)
                                    variant.save()
                                except HTTPError:
                                    print("----Could not save variant({}) from product({})----".format(variant.id,
                                                                                                       product.id))
                                    continue
                                except ServerError:
                                    print("----Could not save variant({}) from product({})----".format(variant.id,
                                                                                                       product.id))
                                    continue
                        self._shopify_ids[row["Item Number"].values[0]] = {
                            "variant_id": variant.id,
                            "product_id": product.id
                        }

                    p = int(100 * i / total)
                    if p not in progress:
                        self.debug("{}%".format(p))
                        progress.append(p)
                    i += 1
                    if i % 1000 == 0:
                        sleep(60 * 5)
        self.debug("100%\n")

    def new_product(self, mill_name, style, short_description, category, color_index):
        """Create a new Shopify product with the given data."""
        new_product = shopify.Product()
        title = "{} {}: {}".format(mill_name, style, short_description)
        if color_index > 0:
            color = ""
            if color_index >= len(self._color_groups):
                color = "Extended Colors {}".format(color_index)
            else:
                color = self._color_groups[color_index]
            title = "{}, {}".format(title, color)
        new_product.title = title
        new_product.vendor = mill_name
        new_product.product_type = category
        sleep(0.5)
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
        if not product_id:
            print("No product!")
        if filename in self._images and self._images[filename].product_id == product_id:
            return self._images[filename]
        else:
            if isinstance(filename, str):
                hires = filename.split(".")
                hires[-2] = ''.join([hires[-2][:-1], 'z'])
                hires = ".".join(hires)
                url = self._image_url(hires)
                # print(url)

                file_location = os.path.join('images', hires)

                opener = urllib.request.build_opener()
                opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                                    '(KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36')]
                urllib.request.install_opener(opener)

                try:
                    urllib.request.urlretrieve(url, file_location)
                    image = shopify.Image({"product_id": product_id})
                except HTTPError:
                    image = None

                if image:
                    with open(file_location, 'rb') as f:
                        encoded = f.read()
                        image.attach_image(encoded, file_location)
                        sleep(0.5)
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

    @staticmethod
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]
