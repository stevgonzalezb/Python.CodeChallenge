import numpy as np
import pandas as pd
import requests as req
from configparser import ConfigParser

def configRead():
    config_file = "config.ini"
    config = ConfigParser()
    config.read(config_file)
    
    print(config.sections())

def concat_categories(row):
    result = str(row['CATEGORY']).lower() + ' | ' + str(row['SUB_CATEGORY'])
    return result

def remove_html_tags(row):
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

def process_csv_files():
    #with req.get("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv") as rq:
    #    with open("C:/cornershop_test/cornershop_code_challenge/resources/products.csv", "wb") as file:
    #        file.write(rq.content)
    #        print(rq.ok)


    #with req.get("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv") as rq:
    #    with open("C:/cornershop_test/cornershop_code_challenge/resources/prices_stock.csv", "wb") as file:
    #        file.write(rq.content)
    #        print(rq.ok)

    df_products = pd.read_csv("C:/cornershop_test/cornershop_code_challenge/resources/products.csv", delimiter='|')
    df_prices = pd.read_csv("C:/cornershop_test/cornershop_code_challenge/resources/prices_stock.csv", delimiter='|')

    joined = df_products.join(df_prices, on='SKU', how='inner', rsuffix='_SKU')

    print(joined[['SKU', 'BUY_UNIT', 'DESCRIPTION_STATUS', 'ORGANIC_ITEM', 'KIRLAND_ITEM', 'FINELINE_NUMBER', 'EAN', 'ITEM_NAME', 'ITEM_DESCRIPTION', 'BRANCH', 'PRICE', 'STOCK']].head())


if __name__ == "__main__":
    configRead()
    #process_csv_files()
