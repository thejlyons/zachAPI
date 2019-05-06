"""Add rate limiting to ShopifyConnection."""
import time

import pyactiveresource.connection
from shopify.base import ShopifyConnection
import sys

def patch_shopify_with_limits():
    """Add limits"""
    func = ShopifyConnection._open

    def patched_open(self, *args, **kwargs):
        """Add limits."""
        error = None
        for _ in range(8):
            error = None
            try:
                return func(self, *args, **kwargs)

            except pyactiveresource.connection.ClientError as e:
                error = e
                if e.response.code == 429:
                    retry_after = float(e.response.headers.get('Retry-After', 8))
                    time.sleep(retry_after)
                else:
                    print(e, file=sys.stderr)
                    raise e
            except pyactiveresource.connection.ServerError as e:
                error = e
                print(e, file=sys.stderr)
                time.sleep(60)

        if error:
            raise ValueError("Could not complete request: {}.".format(error))

    ShopifyConnection._open = patched_open


patch_shopify_with_limits()
