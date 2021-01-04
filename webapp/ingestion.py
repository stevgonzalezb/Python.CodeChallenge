import numpy as np
import pandas as pd
import requests as req
import os
import configparser

#Import the config file
config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../resources/config.ini")
config = configparser.ConfigParser()
config.read(config_path)

#Declare constant variables from config file
MERCHANT_NAME = config['merchant']['name']
BRANCHES = config['merchant']['branches'].split(";")
TOP_INSERT = int(config['source_data']['top_insert'])
INDEX_COL_NAME = config['source_data']['index_col_name']
SORT_COL_NAME = config['source_data']['sort_col_name']
FILL_NA_VALUE = config['source_data']['fill_na_value']
PRODUCTS_URL = config['source_data']['csv_products']
STOCK_PRICES_URL = config['source_data']['csv_stocks_prices']
DELIMITER = config['source_data']['delimiter']
BASE_URL = config['api_client']['base_url']
GRAND_TYPE =config['api_client']['grand_type']
CLIENT_ID = config['api_client']['client_id']
CLIENT_SECRET = config['api_client']['client_secret']
RELATIVE_PATH_PRODUCTS = path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../resources/products.csv")
RELATIVE_PATH_PRICES_STOCKS = path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../resources/prices_stock.csv")

#Method that return the categories concatenated
def concat_categories(row):
    try:
        result = str(row['CATEGORY']).lower() + ' | ' + str(row['SUB_CATEGORY'])
        return result
    except:
        print('ingestion.py >> concat_categories >> Error creating new categories column')

#Method that removes HTML tags from item description
def remove_html_tags(row):
    try:
        s = str(row['ITEM_DESCRIPTION'])
        tag = False
        quote = False
        out = ""

        for c in s:
            if c == '<' and not quote:
                tag = True
            elif c == '>' and not quote:
                tag = False
            elif (c == '"' or c == "'") and tag:
                quote = not quote
            elif not tag:
                out = out + c

        return out
    except:
        print('ingestion.py >> remove_html_tags >> Error removing HTML tags from products description')

#Method that stitches the csv files
def process_csv_files():
    try:
        print("ingestion.py >> process_csv_files >> Start downloading PRODUCTS csv from S3")
        with req.get(PRODUCTS_URL) as rq:
            path = os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRODUCTS)
            with open(path, "wb") as file:
                file.write(rq.content)
                if(rq.ok):
                    print("ingestion.py >> process_csv_files >> Succesfully downloaded PRODUCTS csv")

        print("ingestion.py >> process_csv_files >> Start downloading STOCKS PRICES csv from S3")
        with req.get(STOCK_PRICES_URL) as rq:
            path = os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRICES_STOCKS)
            with open(path, "wb") as file:
                file.write(rq.content)
                if(rq.ok):
                    print("ingestion.py >> process_csv_files >> Succesfully downloaded STOCKS csv")

        print('ingestion.py >> process_csv_files >> Defining indexes and delimiters in files')
        #Setting index column and delimiter 
        df_products = pd.read_csv(RELATIVE_PATH_PRODUCTS, delimiter=DELIMITER, index_col=(INDEX_COL_NAME))
        df_stocks_prices = pd.read_csv(RELATIVE_PATH_PRICES_STOCKS, delimiter=DELIMITER, index_col=(INDEX_COL_NAME))

        #Filterin data
        print('ingestion.py >> process_csv_files >> Filtering data by branches and stocks')
        df_filtered_prices = df_stocks_prices[(df_stocks_prices['STOCK'] > 0) & (df_stocks_prices['BRANCH'].isin(BRANCHES))]
        df_final_data = df_products.join(df_filtered_prices, on=[INDEX_COL_NAME], how='inner', rsuffix='_rSKU', lsuffix='_lSKU')

        #Cleanig data        
        print('ingestion.py >> process_csv_files >> Setting up products categories')
        df_final_data['FULL_CATEGORIES'] = df_final_data.apply(concat_categories, axis = 1)

        print('ingestion.py >> process_csv_files >> Removing HTML tags from descriptions')
        df_final_data['_ITEM_DESCRIPTION'] = df_final_data.apply(remove_html_tags, axis = 1)

        print('ingestion.py >> process_csv_files >> Filling NULL values')
        df_final_data = df_final_data.fillna({"BUY_UNIT": FILL_NA_VALUE, "DESCRIPTION_STATUS": FILL_NA_VALUE, "ORGANIC_ITEM": FILL_NA_VALUE})

        return df_final_data

    except:
        print('ingestion.py >> process_csv_files >> Error processing CSV files')
    
#Implementation to get access token from API provider
def get_access_token():
    print('ingestion.py >> get_access_token >> Getting access token for API')
    token = req.post(BASE_URL+'/oauth/token?client_id='+CLIENT_ID+'&client_secret='+CLIENT_SECRET+'&grant_type='+GRAND_TYPE)

    if token.ok:
        return token.json()['access_token']
    else:
        print('ingestion.py >> get_access_token >> There is an error obtaining the access token')

#Method that exec a GET HTTP method to get all merchants
def get_merchants(access_token):
    print('ingestion.py >> get_merchants >> Getting all merchants from API')
    header = {'token': 'Bearer ' + access_token}
    merchants = req.get(BASE_URL+'/api/merchants', headers=header)

    if merchants.ok:
        return merchants.json()
    else:
        print('ingestion.py >> get_merchants >> There is an error obtaining the merchants')
    
#Method that exec a PUT HTTP method to update specific merchant
def update_merchant_status(access_token, merchant_data, status):
    print('ingestion.py >> update_merchant_status >> Updating merchant status of the store id: ' + merchant_data['id'])
    header = {'token': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    body = {
        "can_be_deleted": merchant_data['can_be_deleted'],
        "can_be_updated": merchant_data['can_be_updated'],
        "id": merchant_data['id'],
        "is_active": status,
        "name": merchant_data['name']
    }
    response = req.put(BASE_URL +'/api/merchants/'+ str(merchant_data['id']), headers=header, json = body)

    if response.ok:
        print('ingestion.py >> update_merchant_status >> Updated correctly >> Store ID: ' + merchant_data['id'])
        return True
    else:
        print('ingestion.py >> update_merchant_status >> There is an error updating the store: ' + merchant_data['id'])

#Method that exec a DELETE HTTP method to delete specific merchant
def delete_merchant(access_token, merchant_id):
    print('ingestion.py >> delete_merchant >> Deleting merchant of the store id: ' + merchant_id)
    header = {'token': 'Bearer ' + access_token}
    response = req.delete(BASE_URL +'/api/merchants/'+ str(merchant_id), headers=header)
    
    if response.ok:
        print('ingestion.py >> delete_merchant >> Deleted correctly >> Store ID: ' + merchant_id)
        return True
    else:
        print('ingestion.py >> delete_merchant >> There is an error deleting the store: ' + merchant_id)

#Method that exec a POST HTTP method to insert products
def update_products(access_token, product, merchant_id):
    header = {'token': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    body = {
	    "merchant_id": merchant_id,
        "sku": str(product.name),
        "barcodes": ["62773501448"],
        "brand": product['BRAND_NAME'],
        "name": product['ITEM_NAME'],
        "description": product['_ITEM_DESCRIPTION'],
        "package": "",
        "image_url": product['ITEM_IMG'],
        "category": product['FULL_CATEGORIES'],
        "url": "",
        "branch_products": [{
    	    "branch": product['BRANCH'],
    	    "stock": product['STOCK'],
    	    "price": product['PRICE']
        }]
    }
    response = req.post(BASE_URL +'/api/products', headers=header, json = body)

    if response.ok:
        print('ingestion.py >> update_products >> Product inserted succesfully >> SKU: ' + str(product.name))
        return True
    else:
        print('ingestion.py >> update_products >> There is an error inserting the product with SKU: ' + str(product.name))

def main():
    richards_data = {}
    beauty_id = ''
    put_flag = False
    delete_flag = False

    try:
        products = process_csv_files()

        access_token = get_access_token()
        if access_token:
            merchants = get_merchants(access_token)

            #Look for the Richard's and Beauty's id in the merchants array
            for merchant in merchants['merchants']:
                if merchant['name'] == MERCHANT_NAME and merchant['is_active'] == False and merchant['can_be_updated'] == True:
                    richards_data = merchant
                    put_flag = True

                if merchant['name'] == 'Beauty' and merchant['can_be_deleted'] == True:
                    beauty_id = merchant['id']
                    delete_flag = True

            #Update the merchant if it exists in the API
            if put_flag:
                update_merchant_status(access_token, richards_data, True)
            else:
                print("ingestion.py >> main >> The merchant cannot be updated. Check if it's able to be modified or if it's already actived")
    
            #Delete the merchant if it exists in the API
            if delete_flag:
                delete_merchant(access_token, beauty_id)
            else:
                print("ingestion.py >> main >> The merchant cannot be updated. Check if it's able to be deleted or if not exists")

            for branch in BRANCHES:
                df_filtered_products = products[products['BRANCH'] == branch].sort_values(by=[SORT_COL_NAME], ascending=False).head(TOP_INSERT)
                df_filtered_products.apply(lambda row: update_products(access_token, row, richards_data['id']), axis=1)

            #Delete CSV files from environment
            if os.path.exists(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRODUCTS)):
                os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRODUCTS))
            if os.path.exists(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRICES_STOCKS)):
                os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRICES_STOCKS))
            print("ingestion.py >> main >> CSV files removed successfully.")

    except Exception as e:
        print("ingestion.py >> main >> Error executing the main method." + e.__class__)


