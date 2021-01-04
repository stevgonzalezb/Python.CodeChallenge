## Introduction
This document is intended to show the configuration process of the app.

### Prerequisites
#### Dependencies
When yo download the project, open a command promt terminal and execute the next command:
	`pip install -r requirements.txt`
	
It will install the next dependecies: 
- numpy==1.19.0
- pandas==1.1.4
- requests==2.25.0
- configparser==5.0.1
- schedule==0.6.0


### Configuration
Open the /resouces/config.ini file, and the its content should have this structure:

| Parameter  | Use  | Value |
| ------------ | ------------ | ------------ |
| source_data.csv_products  | URL from S3 | https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv |
| source_data.csv_stocks_prices  | URL from S3  | https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv  |
| source_data.delimiter  | character used as delimiter |  |
| source_data.top_insert  | top of prodcts to insert in the API  | 100  |
| source_data.index_col_name  | Column's name used as index in both files  | SKU  |
| source_data.sort_col_name | Column's name used to sort the registers descending  | PRICE  |
| source_data.fill_na_value  | Generic value to fill NaN values in data frames  | N/A  |
| api_client.base_url  | API base URL  | http://127.0.0.1:5000  |
| api_client.grand_type  | Grand type to api auth | client_credentials |
| api_client.client_id  |  Client id to api auth |   |
| api_client.client_secret  | Client secret to api auth  |   |
| merchant.name  | Main merchant name  | Richard's  |
|  merchant.branches | Branches to process. You can list several braches separating the values by semicolon( ; ) |  RHSM;MM |
|  cron_job.minutes | Minutes cron  | 30  |