import numpy as np
import pandas as pd
import requests as req
import os
import configparser

#Import the config file
config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../resources/config.ini")
config = configparser.ConfigParser()
config.read(config_path)

#Declare constant variables from config file
MERCHANT_NAME = config['merchant']['name']
PRODUCTS_URL = config['source_data']['csv_products']
STOCK_PRICES_URL = config['source_data']['csv_stocks_prices']
DELIMITER = config['source_data']['delimiter']
BASE_URL = config['api_client']['base_url']
GRAND_TYPE =config['api_client']['grand_type']
CLIENT_ID = config['api_client']['client_id']
CLIENT_SECRET = config['api_client']['client_secret']
RELATIVE_PATH_PRODUCTS = path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../resources/products.csv")
RELATIVE_PATH_PRICES_STOCKS = path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../resources/prices_stock.csv")

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
        df_products = pd.read_csv(RELATIVE_PATH_PRODUCTS, delimiter=DELIMITER, index_col=('SKU'))
        df_stocks_prices = pd.read_csv(RELATIVE_PATH_PRICES_STOCKS, delimiter=DELIMITER, index_col=('SKU'))

        print('ingestion.py >> process_csv_files >> Filtering data by branches and stocks')
        #Filtering data
        df_filtered_prices = df_stocks_prices[(df_stocks_prices['STOCK'] > 0) & ((df_stocks_prices['BRANCH'] == 'RHSM') | (df_stocks_prices['BRANCH'] == 'MM'))]
        df_final_data = df_products.join(df_filtered_prices, on=['SKU'], how='inner', rsuffix='_rSKU', lsuffix='_lSKU')

        #Cleanig data        
        print('ingestion.py >> concat_categories >> Setting up products categories')
        df_final_data['FULL_CATEGORIES'] = df_final_data.apply(concat_categories, axis = 1)

        print('ingestion.py >> remove_html_tags >> Removing HTML tags from descriptions')
        df_final_data['_ITEM_DESCRIPTION'] = df_final_data.apply(remove_html_tags, axis = 1)

        return df_final_data

    except:
        print('ingestion.py >> process_csv_files >> Error processing CSV files')
    


def get_access_token():
    print('ingestion.py >> get_access_token >> Getting access token for API')
    token = req.post(BASE_URL+'/oauth/token?client_id='+CLIENT_ID+'&client_secret='+CLIENT_SECRET+'&grant_type='+GRAND_TYPE)

    if token.ok:
        return token.json()['access_token']
    else:
        print('ingestion.py >> get_access_token >> There are an error obtaining the access token')

def get_merchants(access_token):
    print('ingestion.py >> get_merchants >> Getting all merchants from API')
    header = {'token': 'Bearer ' + access_token}
    merchants = req.get(BASE_URL+'/api/merchants', headers=header)

    if merchants.ok:
        return merchants.json()
    else:
        print('ingestion.py >> get_merchants >> There are an error obtaining the merchants')
    

def update_merchant_status(access_token, merchant_id, status):
    print('ingestion.py >> update_merchant_status >> Updating merchant status of the store id: ' + merchant_id)
    header = {'token': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    body = {
        "can_be_deleted": True,
        "can_be_updated": True,
        "id": merchant_id,
        "is_active": status,
        "name": MERCHANT_NAME
    }
    response = req.put(BASE_URL +'/api/merchants/'+ str(merchant_id), headers=header, json = body)

    if response.ok:
        print('ingestion.py >> update_merchant_status >> Updated correctly >> Store ID: ' + merchant_id)
        return True
    else:
        print('ingestion.py >> update_merchant_status >> There are an error updating the store: ' + merchant_id)

def delete_merchant(access_token, merchant_id):
    print('ingestion.py >> delete_merchant >> Deleting merchant of the store id: ' + merchant_id)
    header = {'token': 'Bearer ' + access_token}
    response = req.delete(BASE_URL +'/api/merchants/'+ str(merchant_id), headers=header)
    
    if response.ok:
        print('ingestion.py >> delete_merchant >> Deleted correcly >> Store ID: ' + merchant_id)
        return True
    else:
        print('ingestion.py >> delete_merchant >> There are an error deleting the store: ' + merchant_id)

def update_products(products):
    print(products.head())


def main():
    print("ingestion.py >> main >> Application initialized correctly! ")
    richards_id = ''
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
                if merchant['name'] == MERCHANT_NAME:
                    richards_id = merchant['id']
                    put_flag = True

                if merchant['name'] == 'Beauty':
                    beauty_id = merchant['id']
                    delete_flag = True

            #Update the merchant if it exists in the API
            if put_flag:
                update_merchant_status(access_token, richards_id, True)
    
            #Delete the merchant if it exists in the API
            if delete_flag:
                delete_merchant(access_token, beauty_id)

            update_products(products)

            os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRODUCTS))
            os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), RELATIVE_PATH_PRICES_STOCKS))

            print("ingestion.py >> main >> CSV files removed successfully.")
    except:
        print("ingestion.py >> main >> Error executing the main method.")

if __name__ == "__main__":
    main()


