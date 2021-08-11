"""API integration for syncing inventories from AlphaBroder to Shopify."""
import os
from api import API
from dotenv import load_dotenv
from datetime import datetime, timedelta
import click

load_dotenv()
last_run = None


def check_should_run():
    """Check if it is the next day and SanMar should run again."""
    global last_run

    now = datetime.utcnow() - timedelta(hours=7)
    if last_run is None or last_run.day != now.day:
        last_run = now
        return True


@click.command()
@click.option('--continuous', '-c', is_flag=True, help='Run continuously. Relevant to updating inventory only.')
@click.option('--limit', '-l', default=5, help='Limit number of imported products.')
@click.option('--download', '-d', is_flag=True, help="Download Files.")
@click.option('--verbose', '-v', is_flag=True, help="Verbose - Show debug statements.")
@click.option('--existing', '-e', is_flag=True, help="Include existing products and re-import them.")
@click.option('--products', '-p', is_flag=True, help="Update products.")
@click.option('--inventory', '-i', is_flag=True, help="Update inventory.")
@click.option('--sanmar', '-s', is_flag=True, help="SanMar. If flag is not present, AlphaBroder settings will be used. "
                                                   "Relevant to updating products only.")
def main(continuous, limit, download, verbose, existing, products, inventory, sanmar):
    """Integration"""
    # TODO: Use flags instead of .env settings for ONLY_THESE
    # 600
    print(f'<{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}>: Begin inventory update.')

    files = 'files'
    if not os.path.exists(files):
        os.mkdir(files)

    images = 'images'
    if not os.path.exists(images):
        os.mkdir(images)

    api = API(download, verbose)

    if products:
        api.update_products(limit=limit, sanmar=sanmar, skip_existing=not existing)
    if inventory:
        if continuous:
            while True:
                # if check_should_run():
                #     print(f'<{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}>: Sanmar inventory update.')
                #     api.update_inventory()
                print(f'<{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}>: AlphaBroder inventory update.')
                api.update_inventory(True)

        else:
            api.update_inventory()

    print(f'<{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}>: Finished inventory update.')


if __name__ == '__main__':
    main()
