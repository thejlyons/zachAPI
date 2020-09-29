import pymongo
from dotenv import load_dotenv
import os
import json
import shopify

load_dotenv()

shop_url = "https://{}:{}@{}.myshopify.com/admin/api/2020-07".format(os.environ["SHOPIFY_API_KEY"],
                                                                     os.environ["SHOPIFY_PASSWORD"],
                                                                     os.environ["SHOPIFY_STORE"])
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.headers.update({'X-Shopify-Access-Token': shopify.ShopifyResource.password })

style = " OR ".join("F280,F282".split(","))
print(style)
query = f'''
{{
  productVariants(first: 250, query: "{style}") {{
    edges {{
      cursor
      node {{
        id
        legacyResourceId
        product {{
          id
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

client = shopify.GraphQL()
result = client.execute(query)
data = json.loads(result)
inventory_item_adjustments = []
for pv in data['data']['productVariants']['edges']:
    node = pv['node']
    quantity = node['inventoryQuantity']
    ii_id = node['inventoryItem']['id']
    pid = node['product']['legacyResourceId']
    inventory_item_adjustments.append(f'{{inventoryItemId: "{ii_id}", availableDelta: 11}}')
    print(quantity)
    print(pid)

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

print(query)
# result = client.execute(query)
print(result)
