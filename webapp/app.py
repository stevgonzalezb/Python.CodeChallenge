import schedule
import time
import ingestion
import os
import configparser

#Import the config file
config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../resources/config.ini")
config = configparser.ConfigParser()
config.read(config_path)

#Declare constant variables from config file
CRON_MINUTES = config['cron_job']['minutes']

print("app.py >> Application initialized correctly! ")
schedule.every(int(CRON_MINUTES)).minutes.do(ingestion.main)

while 1:
    schedule.run_pending()
    time.sleep(10)