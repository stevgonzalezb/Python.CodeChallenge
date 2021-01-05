## Introduction
This document is intended to show the configuration process of the app.

### Prerequisites
#### Dependencies
When yo download the project, open a cmd terminal and execute the next command:
	`pip install -r requirements.txt`
	
It will install the next dependecies: 
- numpy==1.19.0
- pandas==1.1.4
- requests==2.25.0
- configparser==5.0.1
- schedule==0.6.0


### Configuration
Open the /resources/config.ini file, and the its content should have this structure:

| Parameter  | Use  | Value |
| ------------ | ------------ | ------------ |
| source_data.csv_products  | URL from S3 | https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv |
| source_data.csv_stocks_prices  | URL from S3  | https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv  |
| source_data.delimiter  | character used as delimiter | pipe symbol |
| source_data.top_insert  | top of products to insert in the API  | 100  |
| source_data.index_col_name  | Column's name used as index in both files  | SKU  |
| source_data.sort_col_name | Column's name used to sort the registers descending  | PRICE  |
| source_data.fill_na_value  | Generic value to fill NaN values in data frames  | N/A  |
| api_client.base_url  | API base URL  | http://127.0.0.1:5000  |
| api_client.grand_type  | Grand type to api auth | client_credentials |
| api_client.client_id  |  Client id to api auth |   |
| api_client.client_secret  | Client secret to api auth  |   |
| merchant.name  | Main merchant name  | Richard's  |
|  merchant.branches | Branches to process. You can list several branches separating the values by semicolon( ; ) |  RHSM;MM |ccc
|  merchant.packages_units | Packages units to extract from tem description. You can list several units separating the values by semicolon( ; ) |  ML;UN;PZA;GRS;GR,LT |
|  cron_job.minutes | Minutes cron  | 30  |

### Execute test file
You have to execute in a new cmd terminal the next command:
	`python webapp/test.py`

### Execute the app
You have to execute in a new cmd terminal the next command:
	`python webapp/app.py`