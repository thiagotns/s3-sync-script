import os
import sys
import json
import requests
import pandas as pd
import csv
from datetime import datetime
import boto3
import logging

URL_YOUMAIL_API_LIST = "https://dataapi.youmail.com/directory/spammers/v2/partial/since/"
URL_YOUMAIL_API_FULL = "https://dataapi.youmail.com/api/v3/spammerlist/full"
URL_YOUMAIL_API_PARTIAL_HOUR = "https://dataapi.youmail.com/api/v3/spammerlist/partial/"
CSV_FOLDER = "files"
YOUMAIL_FULL_FILENAME = "spam-number-file.csv"
YOUMAIL_PART_FILENAME = "spam-number-file_"
BUCKET_NAME = 'youmail'

#get credentials from credentials.json
def get_credentials():
    with open("credentials.json") as file:
        cf = json.load(file)

    return cf


def get_youmail_api_headers():

    config = get_credentials()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7',
        'Accept': 'application/json',
        #'Accept-Encoding': 'gzip',
        'DataApiSid': config['YOUMAIL_API_SID'],
        'DataApiKey': config['YOUMAIL_API_KEY']
    }
    
    return headers


# +
#get partial spam list by datetime
def get_youmail_partial_list(datetime):
    
    try:
        logging.info(f"[PARTIAL] Downloading partial file (hourly change) from YOUMAIL api: datetime: {datetime}")

        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_PARTIAL_HOUR + datetime, headers=headers)
        result =  response.json()

        logging.info(f"[PARTIAL] Download Finished: totalPhoneNumbersCount = {result['totalPhoneNumbersCount']}")

        return result

    except requests.exceptions.RequestException as e:
        logging.exception(e)
        raise SystemExit(e)

#hourly = get_youmail_partial_list("20211020T160000Z")


# -

#get full spam list
def get_youmail_full_list():
    
    try:
        logging.info("Downloading full file list from YOUMAIL api")

        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_FULL, headers=headers)
    
        result = response.json()

        logging.info(f"Download Finished: totalPhoneNumbersCount = {result['totalPhoneNumbersCount']}")

        return result
    except requests.exceptions.RequestException as e:
        logging.exception(e)
        raise SystemExit(e)
        
    


#get the full list from api, transform, and save it as csv file
def save_youmail_full():

    try:
    
        #get full spam list from youlist API
        data = get_youmail_full_list()

        #transform investigationReasons data
        for d in data['phoneNumbers']:
            if d['investigationReasons']:
                for i in d['investigationReasons']:
                    d[i['name']] = i['certainty']


        df = pd.DataFrame(data['phoneNumbers'])
        df.drop('investigationReasons', axis=1, inplace=True)
        df.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
        
        filename = CSV_FOLDER + "/" + YOUMAIL_FULL_FILENAME
        
        df.to_csv(filename, index=False)

        logging.info(f"File \"{filename}\" saved to local filesystem")

        return filename
    
    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)
        
    return


#mock
def partial_number_operation(number, spam_score, fraud_probability, TCPA_fraud_probability):
    '''
    with open(CSV_FOLDER + "/" + YOUMAIL_FULL_FILENAME, 'rt') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if number == row[0]:
                return 'M'
    return 'A'
    '''
    return '-'


#get partial spam list by now and save it to csv
def save_this_hour_partial_spam_list():
    
    try:
    
        base = datetime.utcnow()
        hour = base.strftime('%Y%m%dT%H0000Z')

        #get the partial file
        diff = get_youmail_partial_list(hour)
        
        #transform investigationReasons data
        for d in diff['phoneNumbers']:
            if d['investigationReasons']:
                for i in d['investigationReasons']:
                    d[i['name']] = i['certainty']

        #prepare dataframe
        df = pd.DataFrame(diff['phoneNumbers'])
        df.drop('investigationReasons', axis=1, inplace=True)
        df.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
        
        df["Number"] = pd.to_numeric(df["Number"])
        
        #calculate Operation field based on last full list (left join) 
        full = pd.read_csv("files/spam-number-file.csv")        

        #Add or Update
        merge = df.merge(full.drop_duplicates(), on=['Number'], how='left', indicator=True)
        df['Operation'] = merge.apply((lambda row: 'A' if row['_merge'] == 'left_only' else 'M'), axis=1)
        
        #Delete or update
        #merge = full.merge(df.drop_duplicates(), on=['Number'], how='left', indicator=True)
        #df['Operation_D'] = merge.apply((lambda row: 'D' if row['_merge'] == 'left_only' else 'M'), axis=1)
        
        filename = CSV_FOLDER + "/" + YOUMAIL_PART_FILENAME + hour + ".csv"

        df.to_csv(filename, index=False)
    
        logging.info(f"[PARTIAL] File \"{filename}\" saved to local filesystem")

        return filename
    
    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)


#upload a file to s3
def upload_file(file_name, sync_tipe = ""):

    logging.info(f"[{sync_tipe}] Uploading file \"{file_name}\" to S3")

    cfg = get_credentials()
    
    s3 = boto3.client('s3', aws_access_key_id = cfg['AWS_ACCESS_KEY'], aws_secret_access_key = cfg['AWS_SECRET_KEY'])

    object_name = os.path.basename(file_name)
    
    try:
    
        s3.upload_file(file_name, BUCKET_NAME, object_name)
        logging.info(f"[{sync_tipe}] Upload Successful (S3): bucket=\"{BUCKET_NAME}\" object_name=\"{file_name}\"")
        return True
    
    except Exception as e:
        logging.exception(e)
        return False


def sync_full():
    try:

        logging.info("[FULL] Full Sync starting...")

        full_csv = save_youmail_full()
        
        result =  upload_file(full_csv, 'FULL')

        logging.info("[FULL] Full Sync Finished")

        return result
    
    except Exception as e:
        logging.exception(e)
        return False


def sync_partial():
    try:
        
        logging.info("[PARTIAL] Hourly Changes Sync starting...")

        partial_csv = save_this_hour_partial_spam_list()
        
        result = upload_file(partial_csv, 'PARTIAL')

        logging.info("[PARTIAL] Hourly Changes Sync finished")
    
        return result

    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)


def main(args):
    
    if len(args) == 0:
        logging.error("Please use FULL or PARTIAL parameter")
        return
    
    if 'FULL' not in args and 'PARTIAL' not in args:
        logging.errorint("Please use FULL or PARTIAL parameter")
        return

    if 'FULL' in args and 'PARTIAL' in args:
        logging.error("Use only one option FULL or PARTIAL")
        return
    
    if 'FULL' in args:
        sync_full()
        
    if 'PARTIAL' in args:
        sync_partial()


if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("------------------    Script Start    ------------------")
    
    #main(sys.argv[1:])
    #main('FULL')
    main('PARTIAL')
    
    logging.info("------------------    Script End      ------------------")