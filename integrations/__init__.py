import pandas as pd

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


df_products = pd.read_csv("C:/cornershop_test/cornershop_code_challenge/resources/products.csv", delimiter='|')
df_products = df_products.set_index('SKU', inplace=False)

df_prices = pd.read_csv("C:/cornershop_test/cornershop_code_challenge/resources/prices_stock.csv", delimiter='|')
df_prices = df_prices.set_index('SKU')

df_filtered_prices = df_prices[(df_prices['STOCK'] > 0) & ((df_prices['BRANCH'] == 'RHSM') | (df_prices['BRANCH'] == 'MM'))]

df_final = df_products.join(df_filtered_prices, on=['SKU'], how='inner', rsuffix='_rSKU', lsuffix='_lSKU')

df_final['FULL_CATEGORIES'] = df_final.apply(concat_categories, axis = 1)
df_final['HTML'] = df_final.apply(remove_html_tags, axis = 1)

