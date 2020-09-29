"""API Class for product and inventory management."""
import os
import re
import sys
import json
import pymongo
import shopify
from ftplib import FTP_TLS, FTP
import urllib.request
from urllib.error import HTTPError, URLError
from pyactiveresource.connection import ResourceNotFound, ServerError, Error, BadRequest
import pandas as pd
from datetime import datetime
from time import sleep
from multiprocessing import Process, Manager, Lock, Queue, Value
import shopify_limits
from unidecode import unidecode
from html import unescape
from ssl import SSLEOFError
from http.client import RemoteDisconnected
from urllib.parse import urlparse


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
    _product_ids = {}
    _color_groups = ["", "Basic Colors", "Traditional Colors", "Extended Colors", "Extended Colors 2",
                     "Extended Colors 3", "Extended Colors 4", "Extended Colors 5", "Extended Colors 6",
                     "Extended Colors 7", "Extended Colors 8", "Extended Colors 9", "Extended Colors 10"]
    _image_url = "https://www.alphabroder.com/media/hires/{}".format
    _product_file_sanmar = 'SanMar_EPDD.csv'
    _product_file = 'AllDBInfoALP_Prod.txt'
    _price_file = 'AllDBInfoALP_PRC_R064.txt'
    _inventory_file = 'inventory-v8-alp.txt'
    _progress = []
    _save = False
    _sanmar = False
    _skip_existing = True
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
        if self._download:
            self.debug("Downloading files.")
            self.prepare_inventory()

        self.debug("Connecting to shopify.")
        shop_url = "https://{}:{}@{}.myshopify.com/admin/api/2020-07".format(os.environ["SHOPIFY_API_KEY"],
                                                                             os.environ["SHOPIFY_PASSWORD"],
                                                                             os.environ["SHOPIFY_STORE"])
        shopify.ShopifyResource.set_site(shop_url)
        shopify.ShopifyResource.headers.update({'X-Shopify-Access-Token': shopify.ShopifyResource.password})

        if not self._db:
            self._db = self.init_mongodb()

        self._product_ids = self._sanitize_records(self._db.products.find())

        # Parse Inventory File
        self.debug("Parsing Inventory Files")
        df_alpha = pd.read_csv(os.path.join('files', self._inventory_file), delimiter=',', engine='python',
                               dtype="string")
        df_sanmar = pd.read_csv(os.path.join('files', self._product_file_sanmar), delimiter=',', engine='python',
                                dtype="string")

        z = 0
        cursor = None
        client = shopify.GraphQL()
        inventory_item_adjustments = []

        while True:
            self.debug("Getting page {}".format(z))

            after = f', after: "{cursor}"' if cursor else ''
            style = ''
            if os.environ.get('ONLY_THESE') is not None:
                style = " OR ".join(os.environ['ONLY_THESE'].split(","))
                style = f', query: "{style}"'
            query = f'''
            {{
              productVariants(first: 250{after}{style}) {{
                edges {{
                  cursor
                  node {{
                    legacyResourceId
                    product {{
                      legacyResourceId
                    }}
                    inventoryQuantity
                    inventoryItem {{
                      id
                    }}
                  }}
                }}
              }}
            }}
            '''

            cursor = None
            data = self.execute_graphql(client, query)
            for pv in data['data']['productVariants']['edges']:
                node = pv['node']
                cursor = pv['cursor']
                pid = node['product']['legacyResourceId']
                vid = node['legacyResourceId']
                quantity = node['inventoryQuantity']
                ii_id = node['inventoryItem']['id']

                if pid in self._product_ids and vid in self._product_ids[pid]:
                    item = self._product_ids[pid][vid]
                    alpha_item = item.get('alpha', None) if isinstance(item, dict) else item
                    sanmar_item = item.get('sanmar', None) if isinstance(item, dict) else item

                    found = False
                    total = 0
                    if alpha_item:
                        alpha_row = df_alpha.loc[df_alpha['Item Number'].isin([alpha_item])]
                        if not alpha_row.empty:
                            found = True
                            total += int(alpha_row[k("Total Inventory", False)].values[0])
                            total -= int(alpha_row["DROP SHIP"].values[0])

                    if sanmar_item:
                        sanmar_row = df_sanmar.loc[df_sanmar[k('Item Number', True)].isin([sanmar_item])]
                        if not sanmar_row.empty:
                            found = True
                            total += int(sanmar_row[k("Total Inventory", True)].values[0])

                    if found:
                        available_delta = total - quantity
                        iia = f'{{inventoryItemId: "{ii_id}", availableDelta: {available_delta}}}'
                        inventory_item_adjustments.append(iia)
                        if len(inventory_item_adjustments) == 100:
                            self.update_inventory_items(client, inventory_item_adjustments)
                            inventory_item_adjustments = []
                # else:
                #     self.debug(f"https://bulkthreads.myshopify.com/admin/products/{pid}/variants/{vid}")

            z += 1

            if cursor is None:
                break

        self.update_inventory_items(client, inventory_item_adjustments)
        # TODO: set to 0 products that weren't found
        self._clean()

    @staticmethod
    def save_thread(products, is_last):
        """Thread for saving a list of products."""
        import shopify
        import shopify_limits

        shop_url = "https://{}:{}@{}.myshopify.com/admin/api/2020-07".format(os.environ["SHOPIFY_API_KEY"],
                                                                             os.environ["SHOPIFY_PASSWORD"],
                                                                             os.environ["SHOPIFY_STORE"])

        total = len(products)
        progress = []
        for i, product in enumerate(products):
            try:
                product.save()
            except HTTPError:
                pass
            except ResourceNotFound:
                pass
            except (BadRequest, ValueError):
                sleep(40)
                try:
                    product.save()
                except HTTPError:
                    pass
                except ResourceNotFound:
                    pass
                except (BadRequest, ValueError):
                    sleep(40)
                    s = "Coudn't save {}".format(product.id)
                    print("<{}>: {}%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), s))

            p = int(100 * i / total)
            if is_last and p % 5 == 0 and p not in progress:
                print("<{}>: {}%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), p))
                progress.append(p)
        if is_last:
            print("<{}>: 100%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    def update_products(self, limit=0, sanmar=False, skip_existing=True):
        """Update all products."""
        self._sanmar = sanmar
        if self._sanmar:
            self._categories = os.environ['CATEGORIES_SANMAR'].split(",")
        self._skip_existing = skip_existing

        if self._download:
            self.debug("Downloading files.")
            self.prepare_products()

        self.debug("Connecting to shopify.")
        shop_url = "https://{}:{}@{}.myshopify.com/admin/api/2020-07".format(os.environ["SHOPIFY_API_KEY"],
                                                                             os.environ["SHOPIFY_PASSWORD"],
                                                                             os.environ["SHOPIFY_STORE"])
        shopify.ShopifyResource.set_site(shop_url)

        self.debug("Retrieving database entries.")
        # changes = self.find_changes()

        if not self._db:
            self._db = self.init_mongodb()

        inventory_store = self._sanitize_records(self._db.inventory.find())
        self._product_ids = self._sanitize_records(self._db.products.find())

        # Parse Product File
        self.debug("Parsing Product File")

        self._load_product_file(inventory_store, limit)

        self.debug("Processing products.")
        total = self._inventory.shape[0]
        progress = []
        for i, (index, item) in enumerate(self._inventory.iterrows()):
            if 'Drop Ship' in item[self.k("Mill Name")]:
                continue
            if item[self.k("Style")] not in self._current_products:
                all_products = shopify.Product.find(limit=250, vendor=item[self.k("Mill Name")])
                products = []
                for product in all_products:
                    if item[self.k("Style")] in product.title:
                        style = product.title.replace(',', '').split(' ')
                        if item[self.k("Style")] in style:
                            if len(product.options) > 0 and product.options[0].name == 'Color':
                                for x in range(len(product.variants)):
                                    product.variants[x].option2, product.variants[x].option1 = (
                                        product.variants[x].option1, product.variants[x].option2
                                    )
                                product.options.reverse()
                                product.save()
                                # print("----{} needs checked.----".format(item[self.k("Style")]))
                                self._styles_to_fix.append(item[self.k("Style")])
                            if len(product.variants) == 1 and product.variants[0].title == 'Default Title':
                                product.variants = []
                            products.append(product)
                self._current_products[item[self.k("Style")]] = products
            of_color = self._inventory.loc[(self._inventory[self.k("Style")] == item[self.k("Style")])
                                           & (self._inventory[self.k("Color Name")] == item[self.k("Color Name")])
                                           ].shape[0]
            self.process_item(item, of_color)
            p = int(100 * i / total)
            if p % 10 == 0 and p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)
        self.debug("100%\n")

        self.debug("Saving new products.")
        self.start_save_processes()

        if self._save:
            self.debug("Updating Database.")
            for key, item in inventory_store.items():
                self._shopify_ids[str(key)] = item
            self._db.inventory.delete_many({})
            self._db.inventory.insert_one(self._shopify_ids)
            self._db.products.delete_many({})
            self._db.products.insert_one(self._product_ids)
        else:
            self.debug("Errors found. Skipping save.")
        self._clean()

        print(", ".join(self._styles_to_fix))

    def process_item(self, item, of_color):
        """Check if item already exists. If not, create a new variant and add it."""
        skip = False

        similar_variants = 0
        for p in self._current_products[item[self.k("Style")]]:
            # if p.product_type != item[self.k("Category")]:
            #     skip = True
            #     continue
            # skip = False
            color_option = ""
            color = item[self.k("Color Name")].lower()
            size_option = ""
            size = item[self.k("Size")].lower()

            if len(p.options) >= 2:
                for option in p.options:
                    if option.name == 'Color':
                        color_option = 'option{}'.format(option.position)
                    if option.name == 'Size':
                        size_option = 'option{}'.format(option.position)
            else:
                size_option = 'option1'
                color_option = 'option2'

            if color_option and size_option:
                variant = None
                for v in p.variants:
                    if v.attributes[color_option].lower() == color \
                            and v.attributes[size_option].lower() == size:
                        variant = v
                        break
                if variant:
                    skip = True
                    if variant.id:
                        if self._sanmar:
                            price = self.get_price(item)
                            if price != 0 and price != "":
                                variant.price = price

                        self._shopify_ids[str(item[self.k("Item Number")])] = {
                            "variant_id": variant.id,
                            "product_id": p.id
                        }
                        key = 'sanmar' if self._sanmar else 'alpha'
                        inum = str(item[self.k("Item Number")])
                        self._product_ids.setdefault(str(p.id), {}).setdefault(str(variant.id), {})[key] = inum
                    break
        product = None
        for p in self._current_products[item[self.k("Style")]]:
            if len(p.variants) + of_color - similar_variants < 100:
                product = p
                break

        if not skip:
            if not product:
                product = self.new_product(item[self.k("Mill Name")], item[self.k("Style")],
                                           self.get_description_short(item), item[self.k("Full Feature Description")],
                                           item[self.k("Category")], len(self._current_products[item[self.k("Style")]]))

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
            color = item[self.k("Color Name")].lower()
            similar_variants = len([v for v in product.variants if str(v.attributes[color_option]).lower() == color])
            if len(product.variants) + of_color - similar_variants >= 100:
                for x in range(len(self._current_products[item[self.k("Style")]])):
                    if product.id == self._current_products[item[self.k("Style")]][x].id:
                        self._current_products[item[self.k("Style")]][x] = product
                        break

                size_option = 'option1'
                color_option = 'option2'
                product = self.new_product(item[self.k("Mill Name")], item[self.k("Style")],
                                           self.get_description_short(item), item[self.k("Full Feature Description")],
                                           item[self.k("Category")], len(self._current_products[item[self.k("Style")]]))

            price = self.get_price(item)

            variant = shopify.Variant({color_option: item[self.k("Color Name")].title(),
                                       size_option: item[self.k("Size")], 'product_id': product.id})
            self._images[item[self.k("Front of Image Name")]] = {
                "product_id": product.id
            }
            if price != 0 and price != "":
                variant.price = price
            product.variants.append(variant)

            found = False
            for x in range(len(self._current_products[item[self.k("Style")]])):
                if product.id == self._current_products[item[self.k("Style")]][x].id:
                    self._current_products[item[self.k("Style")]][x] = product
                    found = True
                    break
            if not found:
                self._current_products[item[self.k("Style")]].append(product)

    def start_save_processes(self):
        """Create Processes that will save all the changes."""
        self.debug("Setting metafields.")
        total = len(self._current_products.keys())
        progress = []
        for i, key in enumerate(self._current_products):
            if not self._current_products[key]:
                continue
            main_product = ""
            for product in self._current_products[key]:
                if product.body_html:
                    main_product = product.handle
                    break
            if not main_product:
                main_product = self._current_products[key][0].handle
            other_products = [product.handle for product in self._current_products[key] if product.handle != main_product]
            for x in range(len(self._current_products[key])):
                metafields = self._current_products[key][x].metafields()
                mp = False
                op = False
                for mf in metafields:
                    if mf.namespace == 'api_integration':
                        if mf.key == 'main_product':
                            mp = True
                            mf.value = main_product
                        elif mf.key == 'other_product':
                            op = True
                            if other_products:
                                mf.value = other_products
                        mf.save()

                if not mp:
                    self._current_products[key][x].add_metafield(shopify.Metafield({
                        'key': 'main_product',
                        'value': main_product,
                        'value_type': 'string',
                        'namespace': 'api_integration'
                    }))
                if not op and other_products:
                    self._current_products[key][x].add_metafield(shopify.Metafield({
                        'key': 'other_products',
                        'value': ",".join(other_products),
                        'value_type': 'string',
                        'namespace': 'api_integration'
                    }))
            p = int(100 * i / total)
            if p % 5 == 0 and p not in progress:
                self.debug("{}%".format(p))
                progress.append(p)

        self.debug("Starting processes.")
        processes = []

        manager = Manager()
        ns = manager.Namespace()
        ns.products = [(k, i) for k, i in self._current_products.items() if i]
        ns.progress = []
        ns.shopify_ids = self._shopify_ids
        shopify_ids = manager.dict()
        ns.skip_existing = self._skip_existing
        ns.inventory = self._inventory
        index = Value("i", 0)
        total = len(ns.products)

        r = int(os.environ["NUM_THREADS"]) if int(os.environ["NUM_THREADS"]) < total else total
        for i in range(r):
            p = Process(target=self.save_new_products, args=(ns, index, shopify_ids, total, self._sanmar))
            p.daemon = True
            p.start()
            processes.append(p)

        self._save = True
        for p in processes:
            p.join()
            if p.exitcode > 0:
                self._save = False

        sa = 'sanmar' if self._sanmar else 'alpha'
        for key, item in shopify_ids.items():
            key = str(key)
            self._shopify_ids[key] = item
            self._product_ids.setdefault(str(item["product_id"]), {}).setdefault(str(item["product_id"]), {})[sa] = key

    def update_inventory_items(self, client, inventory_item_adjustments):
        """Bulk updates."""

        nl = '\n'
        query = f'''
          mutation {{
              inventoryBulkAdjustQuantityAtLocation(
                locationId: "gid://shopify/Location/{os.environ['SHOPIFY_LOCATION']}",
                inventoryItemAdjustments: [
                  {f',{nl}'.join(inventory_item_adjustments)}
                  ]) {{

                inventoryLevels {{
                  available
                }}
              }}
            }}
        '''

        self.execute_graphql(client, query)

    def execute_graphql(self, client, query, retries=0):
        """Execute graphql query and wait if necessary."""
        if retries > 4:
            self.debug('Could not complete call. Max retries met.', True)
        try:
            result = client.execute(query)
            result = json.loads(result)
        except urllib.error.HTTPError as e:
            self.debug('Caught: Internal Server Error. Retrying in 1 minute.', True)
            sleep(60)
            retries += 1
            return self.execute_graphql(client, query, retries)

        # {'errors': [{'message': 'Throttled', 'extensions': {'code': 'THROTTLED',
        #                                                     'documentation': 'https://help.shopify.com/api/graphql-admin-api/graphql-admin-api-rate-limits'}}],
        #  'extensions': {'cost': {'requestedQueryCost': 752, 'actualQueryCost': None,
        #                          'throttleStatus': {'maximumAvailable': 1000.0, 'currentlyAvailable': 744,
        #                                             'restoreRate': 50.0}}}}

        if 'errors' in result:
            cost = result['extensions']['cost']
            sleep_for = (cost['requestedQueryCost'] + 10 - cost['throttleStatus']['currentlyAvailable'])
            sleep_for /= cost['throttleStatus']['restoreRate']
            sleep(sleep_for)

            return self.execute_graphql(client, query)
        else:
            return result

    @staticmethod
    def save_new_products(ns, index, shopify_ids, total, sanmar):
        """Loop through self._current_products and save the last in each list"""
        import shopify
        import shopify_limits

        def find_image(filename, product_id, sanmar):
            """Check if image in self._images otherwise download and create it."""
            if isinstance(filename, str):
                hires = filename.split(".")
                hires[-2] = ''.join([hires[-2][:-1], 'z'])
                hires = ".".join(hires)
                url = "https://www.alphabroder.com/media/hires/{}".format(hires) if not sanmar else filename

                if sanmar:
                    a = urlparse(filename)
                    hires = os.path.basename(a.path)

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
                        try:
                            image.save()
                        except RemoteDisconnected:
                            sleep(300)
                            try:
                                image.save()
                            except RemoteDisconnected:
                                pass
                return image
            else:
                return None

        shop_url = "https://{}:{}@{}.myshopify.com/admin/api/2020-07".format(os.environ["SHOPIFY_API_KEY"],
                                                                             os.environ["SHOPIFY_PASSWORD"],
                                                                             os.environ["SHOPIFY_STORE"])
        shopify.ShopifyResource.set_site(shop_url)

        current = 0
        while True:
            with index.get_lock():
                current = index.value
                index.value += 1
            if current >= total:
                break

            style, products = ns.products[current]

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
                try:
                    product.save()
                except SSLEOFError:
                    pass
                except URLError:
                    pass
                except (HTTPError, Error, ValueError):
                    sleep(30)
                    try:
                        product.save()
                    except SSLEOFError:
                        pass
                    except URLError:
                        pass
                    except (HTTPError, Error, ValueError):
                        sleep(30)
                        e = "Could not save {}".format(product.handle)
                        print("<{}>: {}%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))
                        continue

                images = {}
                for variant in product.variants:
                    color_name = str(variant.attributes[color_option]).upper()
                    if sanmar:
                        color_name = str(variant.attributes[color_option]).title()

                    row = ns.inventory.loc[
                        (ns.inventory[k("Style", sanmar)] == style)
                        & (ns.inventory[k("Color Name", sanmar)] == color_name)
                        & (ns.inventory[k("Size", sanmar)] == variant.attributes[size_option])
                        ]
                    if not row.empty and row[k("Item Number", sanmar)].values[0] not in ns.shopify_ids:
                        fn = row[k("Front of Image Name", sanmar)].values[0]
                        image = images.get(fn, None)
                        if not image:
                            image = find_image(fn, product.id, sanmar)
                            images[fn] = image
                        if image:
                            variant.image_id = image.id
                            try:
                                variant.save()
                                shopify_ids[row[k("Item Number", sanmar)].values[0]] = {
                                    "variant_id": variant.id,
                                    "product_id": product.id
                                }
                            except SSLEOFError:
                                pass
                            except URLError:
                                pass
                            except Error:
                                sleep(300)
                                try:
                                    variant.save()
                                    shopify_ids[row[k("Item Number", sanmar)].values[0]] = {
                                        "variant_id": variant.id,
                                        "product_id": product.id
                                    }
                                except SSLEOFError:
                                    pass
                                except URLError:
                                    pass
                                except Error:
                                    sleep(300)
                                    e = "Could not save {}".format(row[k("Item Number", sanmar)].values[0])
                                    print("<{}>: {}%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))
            p = int(100 * current / total)
            if p not in ns.progress:
                print("<{}>: {}%".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), p))
                ns.progress.append(p)

    def new_product(self, mill_name, style, short_description, full_description, category, color_index):
        """Create a new Shopify product with the given data."""
        new_product = shopify.Product()
        title = short_description
        if not self._sanmar:
            title = "{} {}: {}".format(mill_name, style, short_description)
        if color_index > 0:
            color = ""
            if color_index >= len(self._color_groups):
                color = "Extended Colors {}".format(color_index)
            else:
                color = self._color_groups[color_index]
            title = "{}, {}".format(title, color)
        new_product.title = title
        desc_split = full_description.split("|") if self._sanmar else full_description.split(";")
        new_product.body_html = "<ul><li>{}</li></ul>".format("</li><li>".join(
            [li.strip() for li in desc_split]))
        new_product.vendor = mill_name
        new_product.product_type = category
        new_product.save()
        new_product.variants = []
        return new_product

    def _load_product_file(self, inventory_store, limit):
        """Load in product files"""
        pf = self._product_file_sanmar if self._sanmar else self._product_file
        delimiter = ',' if self._sanmar else '^'
        self._inventory = pd.read_csv(os.path.join('files', pf), delimiter=delimiter, engine='python', dtype="string")
        self._inventory = self._inventory.loc[self._inventory[self.k('Category')].isin(self._categories)
                                              & ~self._inventory[self.k('Mill Name')].str.contains('Drop Ship',
                                                                                                   flags=re.IGNORECASE,
                                                                                                   regex=True)]

        if self._skip_existing:
            self._inventory = self._inventory.loc[~self._inventory[self.k('Item Number')].isin(inventory_store.keys())]
        if os.environ.get('ONLY_THESE') is not None:
            these = os.environ['ONLY_THESE'].split(",")
            self._inventory = self._inventory.loc[self._inventory[self.k('Style')].isin(these)]

        if self._sanmar and os.environ.get('BRANDS_SANMAR') is not None:
            brands = os.environ.get('BRANDS_SANMAR').split(',')
            self._inventory = self._inventory.loc[self._inventory[self.k('Mill Name')].isin(brands)]

        self._inventory = self._inventory.replace({self.k("Mill Name"): {'Bella + Canvas': 'Bella+Canvas'}})

        self._inventory.sort_values(self.k('Style'))

        self.debug(f"Found: {self._inventory.shape[0]}")
        if limit > 0:
            self._inventory = self._inventory.head(limit)
        self.debug(f"Importing: {self._inventory.shape[0]}")

    def get_description_short(self, item):
        """Get item short description."""
        if self._sanmar:
            return unescape(item["PRODUCT_TITLE"])
        else:
            return unidecode(item["Short Description"])

    def get_price(self, item):
        """Get item price."""
        if self._sanmar:
            return float(item['PIECE_PRICE'])
        else:
            if self._prices is None:
                self._prices = pd.read_csv(os.path.join('files', self._price_file), delimiter='^', engine='python',
                                           dtype="string")

            price = self._prices.loc[self._prices["Item Number "] == item["Item Number"]]
            if not price.empty:
                price = price["Piece"].values[0]
                try:
                    int(price)
                except ValueError:
                    price = 0
            else:
                price = 0
            return price

    def k(self, key):
        """Get key relative to sanmar or alpha product csvs."""
        return k(key, self._sanmar)

    def prepare_products(self):
        """Prepare for updating products by downloading relevant files."""
        if self._sanmar:
            self.download_sanmar(self._product_file_sanmar)
        else:
            self.download_alpha(self._product_file)
            self.download_alpha(self._price_file)

    def prepare_inventory(self):
        """Prepare for updating product inventory by downloading relevant files."""
        self.download_alpha(self._inventory_file)
        self.download_sanmar(self._product_file_sanmar)

    def download_alpha(self, filename):
        """Download the given file."""
        domain = os.environ["FTP_DOMAIN_ALPHA"]
        user = os.environ["FTP_USERNAME_ALPHA"]
        passwd = os.environ["FTP_PASSWORD_ALPHA"]

        ftp = FTP_TLS(domain)
        ftp.login(user=user, passwd=passwd)

        self.download_file(ftp, filename)

        ftp.quit()

    def download_sanmar(self, filename):
        """Download the given file."""
        domain = os.environ["FTP_DOMAIN_SANMAR"]
        user = os.environ["FTP_USERNAME_SANMAR"]
        passwd = os.environ["FTP_PASSWORD_SANMAR"]

        ftp = FTP(domain)
        ftp.login(user=user, passwd=passwd)

        self.download_file(ftp, filename, dir='SanMarPDD/')

        ftp.quit()

    # def find_images(self):
    #     """Find all images."""
    #     total = len(self._images.keys())
    #     progress = []
    #     for i, (filename, image_data) in enumerate(self._images.items()):
    #         self.find_image(filename, image_data["product_id"])
    #         p = int(100 * i / total)
    #         if p not in progress:
    #             self.debug("{}%".format(p))
    #             progress.append(p)

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
            return
            for f in [self._inventory_file, self._product_file, self._price_file, self._product_file_sanmar]:
                if os.path.isfile(os.path.join('files', f)):
                    os.unlink(os.path.join('files', f))

    @staticmethod
    def init_mongodb():
        """Initialize MongoDB Client."""
        client = pymongo.MongoClient(os.environ["MONGODB_URL"])
        return client.bulkthreads

    @staticmethod
    def _sanitize_records(records):
        """Return records without the default mongodb _id to avoid conflicts on save"""
        try:
            records = [item for item in records][0]
        except IndexError:
            records = []
        records = {key: records[key] for key in records if key != '_id'}
        return records

    def download_file(self, ftp, filename, dir=''):
        """Download given file from global FTP server."""

        download_to = os.path.join('files', filename)
        self.debug("Downloading '{}' to: {}".format(filename, download_to))

        local_file = open(download_to, 'wb')
        ftp.retrbinary(f'RETR {dir}{filename}', local_file.write)
        local_file.close()

    def debug(self, msg, force=False):
        """Method for printing debug messages."""
        if self._debug or force:
            print("<{}>: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))

    @staticmethod
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]


def k(key, sanmar):
    """Get key relative to sanmar or alpha product csvs."""
    if sanmar:
        return {
            'Category': 'CATEGORY_NAME',
            'Mill Name': 'MILL',
            'Item Number': 'UNIQUE_KEY',
            'Style': 'STYLE#',
            'Color Name': 'COLOR_NAME',
            'Front of Image Name': 'FRONT_MODEL_IMAGE_URL',
            'Size': 'SIZE',
            'Full Feature Description': 'PRODUCT_DESCRIPTION',
            'Total Inventory': 'QTY'
        }[key]
    else:
        return key
