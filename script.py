import os
import sys
import json
import requests
import pandas as pd
import csv
from datetime import datetime, timedelta
import boto3
import logging
import glob

os.chdir("/home/thiago/s3-sync-script/")
URL_YOUMAIL_API_LIST = "https://dataapi.youmail.com/directory/spammers/v2/partial/since/"
URL_YOUMAIL_API_FULL = "https://dataapi.youmail.com/api/v3/spammerlist/full"
URL_YOUMAIL_API_PARTIAL_HOUR = "https://dataapi.youmail.com/api/v3/spammerlist/partial/"
CSV_FOLDER = "/home/thiago/s3-sync-script/files"
YOUMAIL_FULL_FILENAME = "FULL_spam-number-file_"
YOUMAIL_PART_FILENAME = "NETCHANGE_spam-number-file_"
YOUMAIL_FULL_NETCHANGE_PART_FILENAME = "FULL_NETCHANGE_spam-number-file_"
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
        logging.info(f"[NETCHANGE] Downloading partial file (hourly change) from YOUMAIL api: datetime: {datetime}")

        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_PARTIAL_HOUR + datetime, headers=headers)
        result =  response.json()

        logging.info(f"[NETCHANGE] Download Finished: totalPhoneNumbersCount = {result['totalPhoneNumbersCount']}")

        return result

    except requests.exceptions.RequestException as e:
        logging.exception(e)
        raise SystemExit(e)

#hourly = get_youmail_partial_list("20211020T160000Z")


# -

#get full spam list
def get_youmail_full_list():
    
    try:
        logging.info("[FULL] Downloading full file list from YOUMAIL api")

        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_FULL, headers=headers)
    
        result = response.json()

        logging.info(f"[FULL] Download Finished: totalPhoneNumbersCount = {result['totalPhoneNumbersCount']}")

        return result
    except requests.exceptions.RequestException as e:
        logging.exception(e)
        raise SystemExit(e)
        
    


#get the full list from api, transform, and save it as csv file
def save_youmail_full():

    try:

        base = datetime.utcnow()
        today = base.strftime('%Y%m%d')
    
        #get full spam list from youlist API
        data = get_youmail_full_list()

        #debug
        with open(f"{CSV_FOLDER}/YOUMAIL_FULL_{today}.txt", 'w') as f:
            json.dump(data, f)

        #transform investigationReasons data
        for d in data['phoneNumbers']:
            if d['investigationReasons']:
                for i in d['investigationReasons']:
                    d[i['name']] = i['certainty']


        df = pd.DataFrame(data['phoneNumbers'])
        df.drop('investigationReasons', axis=1, inplace=True)
        df.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']

        filename = CSV_FOLDER + "/" + YOUMAIL_FULL_FILENAME + today + ".csv"
        
        df.to_csv(filename, index=False)

        logging.info(f"[FULL] File \"{filename}\" saved to local filesystem")

        return filename
    
    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)
        
    return

#get partial spam list by now and save it to csv
def save_this_hour_partial_spam_list():
    
    try:
    
        base = datetime.utcnow()
        hour = base.strftime('%Y%m%dT%H0000Z')
        today = datetime.utcnow().strftime('%Y%m%d')
        hour_to_filename = datetime.utcnow().strftime('%Y%m%d%H00')

        #hora = '03'
        #dia = '20211123'

        #hour = f'{dia}T{hora}0000Z'
        #today = f'{dia}'
        #hour_to_filename = f'{dia}{hora}00'

        #get the partial file
        diff = get_youmail_partial_list(hour)

        #debug
        with open(f"{CSV_FOLDER}/YOUMAIL_NETCHANGE_{hour_to_filename}.txt", 'w') as f:
            json.dump(diff, f)
        
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
        
        filename = CSV_FOLDER + "/" + YOUMAIL_PART_FILENAME + hour_to_filename + ".csv"

        l_prev = glob.glob(f"{CSV_FOLDER}/NETCHANGE_*{today}*.csv")
        l_tmp = []

        for l in l_prev:
            if l == filename:
                continue

            tmp = pd.read_csv(l)
            #tmp['ref'] = l
            l_tmp.append(tmp)

        if len(l_tmp) > 0:
            df_prev = pd.concat(l_tmp, axis=0, ignore_index=True)

            #calculate Operation field based on last full list (left join) 

            #Add or Update
            logging.info(f"[NETCHANGE] Getting added and modified numbers")
            merge = df.merge(df_prev.drop_duplicates(), on=['Number'], how='left', indicator=True)
            #df['Operation'] = merge.apply((lambda row: 'A' if row['_merge'] == 'left_only' else 'M'), axis=1)
            merge = merge.query('_merge == \'left_only\'')[['Number', 'SpamScore_x', 'FraudProbability_x', 'TCPAFraudProbability_x']]
            merge.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
            merge['Operation'] = 'A'
            base = merge

            merge = df.merge(df_prev.drop_duplicates(), on=['Number'], how='left', indicator=True)
            merge = merge.query('_merge != \'left_only\'')[['Number', 'SpamScore_x', 'FraudProbability_x', 'TCPAFraudProbability_x']]
            merge.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
            merge['Operation'] = 'M'
            base = base.append(merge)

            #merge.to_csv('files/merged.csv')

            #Delete
            logging.info(f"[NETCHANGE] Getting deleted number")
            merge = df_prev.drop_duplicates().merge(df.drop_duplicates(), on=['Number'], how='left', indicator=True)
            merge = merge.query('_merge == \'left_only\'')[['Number', 'SpamScore_x', 'FraudProbability_x', 'TCPAFraudProbability_x']]
            merge.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
            merge['Operation'] = 'D'
            base = base.append(merge)

            df = base

            df_full = df_prev.append(df)

        else:
            df['Operation'] = 'A' #first hour
            df_full = df
            #df_full['ref'] = filename

        df.drop_duplicates(inplace=True)
        df_full.drop_duplicates(inplace=True)

        df.to_csv(filename, index=False)
        logging.info(f"[NETCHANGE] File \"{filename}\" saved to local filesystem")

        filename_full_netchange = save_full_netchange(hour_to_filename)

        return filename, filename_full_netchange
    
    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)


def save_full_netchange(hour_to_filename):
    today = hour_to_filename[0:-4]

    filename_full_netchange = CSV_FOLDER + "/" + YOUMAIL_FULL_NETCHANGE_PART_FILENAME + hour_to_filename + ".csv"

    logging.info(f"[NETCHANGE] Genarating full netchange to {hour_to_filename}")

    l_full = glob.glob(f"{CSV_FOLDER}/YOUMAIL_NETCHANGE_*{today}*.txt")
    l_tmp = []

    for l in l_full:
        with open(l) as f:
            data = json.load(f)
            for d in data['phoneNumbers']:
                if d['investigationReasons']:
                    for i in d['investigationReasons']:
                        d[i['name']] = i['certainty']

            #prepare dataframe
            df = pd.DataFrame(data['phoneNumbers'])
            df.drop('investigationReasons', axis=1, inplace=True)
            df.columns = ['Number', 'SpamScore', 'FraudProbability', 'TCPAFraudProbability']
            df["Number"] = pd.to_numeric(df["Number"])

            l_tmp.append(df)
    
    result = pd.concat(l_tmp, axis=0, ignore_index=True)

    result.to_csv(filename_full_netchange, index=False)

    logging.info(f"[NETCHANGE] FULL File \"{filename_full_netchange}\" saved to local filesystem")

    return filename_full_netchange


#upload a file to s3
def upload_file(file_name, sync_tipe = ""):

    logging.info(f"[{sync_tipe}] Uploading file \"{file_name}\" to S3")

    cfg = get_credentials()
    
    s3 = boto3.client('s3', aws_access_key_id = cfg['AWS_ACCESS_KEY'], aws_secret_access_key = cfg['AWS_SECRET_KEY'])

    object_name = os.path.basename(file_name)
    
    try:
    
        s3.upload_file(file_name, BUCKET_NAME, object_name)
        logging.info(f"[{sync_tipe}] Upload Successful (S3): bucket=\"{BUCKET_NAME}\" object_name=\"{object_name}\"")
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
        
        logging.info("[NETCHANGE] Hourly Changes Sync starting...")

        partial_csv, full = save_this_hour_partial_spam_list()
        
        result = upload_file(partial_csv, 'NETCHANGE')
        result = upload_file(full, 'NETCHANGE_FULL')

        logging.info("[NETCHANGE] Hourly Changes Sync finished")
    
        return result

    except Exception as e:
        logging.exception(e)
        raise SystemExit(e)

def delete_obsolete_files():
    
    logging.info("[CLEAN] Cleaning up the obsolete files...")

    base = datetime.utcnow()
    today = datetime.utcnow().strftime('%Y%m%d')
    yesterday = (datetime.utcnow() - timedelta(1)).strftime('%Y%m%d')
    hour_to_filename = datetime.utcnow().strftime('%Y%m%d%H00')

    logging.info(f"[CLEAN] Today: {today}")
    logging.info(f"[CLEAN] Yesterday: {yesterday}")


    #backups (always keep the files another day)
    files = glob.glob(f"{CSV_FOLDER}/*.backup")
    if not files:
        logging.info("[CLEAN] There is no backup files to delete")
    for f in files:
        logging.info("[CLEAN] Removing " + f)
        os.remove(f)

    #netchange - rename to be deleted tomorrow
    files = glob.glob(f"{CSV_FOLDER}/NETCHANGE_*{yesterday}*.csv")
    if not files:
        logging.info("[CLEAN] There is no netchange files to delete")
    for f in files:
        os.rename(f,  f + ".backup")
        logging.info("[CLEAN] Renaming " + f + ".backup")

    #full - rename to be deleted tomorrow
    files = glob.glob(f"{CSV_FOLDER}/FULL_*{yesterday}*.csv")
    if not files:
        logging.info("[CLEAN] There is no full files to delete")
    for f in files:
        os.rename(f,  f + ".backup")
        logging.info("[CLEAN] Renaming " + f + ".backup")

    #youmail - rename to be deleted tomorrow
    files = glob.glob(f"{CSV_FOLDER}/YOUMAIL_*{yesterday}*")
    if not files:
        logging.info("[CLEAN] There is no youmail files to delete")
    for f in files:
        os.rename(f,  f + ".backup")
        logging.info("[CLEAN] Renaming " + f + ".backup")

    logging.info("[CLEAN] Clean up finished.")
    
    return


def main(args):
    
    if len(args) == 0:
        logging.error("Please use FULL or NETCHANGE or CLEAN parameter")
        return
    
    if 'FULL' not in args and 'NETCHANGE' not in args and 'CLEAN' not in args:
        logging.error("Please use FULL or NETCHANGE parameter or CLEAN parameter")
        return

    if 'FULL' in args and 'NETCHANGE' in args and 'CLEAN' in args:
        logging.error("Use only one option FULL, NETCHANGE or CLEAN")
        return
    
    if 'FULL' in args:
        sync_full()
        
    if 'NETCHANGE' in args:
        sync_partial()

    if 'CLEAN' in args:
        delete_obsolete_files()

if __name__ == "__main__":
    
    hour_to_filename = datetime.now().strftime('%Y%m%d%H%M%S')

    prefix = sys.argv[1:][0] if 'FULL' in sys.argv[1:] or 'NETCHANGE' in sys.argv[1:] or 'CLEAN' in sys.argv[1:] else 'INVALID_PARAMETER'

    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(f"/home/thiago/s3-sync-script/log/s3-sync-youmail_{hour_to_filename}_{prefix}.log"),
                            logging.StreamHandler()
                        ])


    logging.info("------------------    Script Start    ------------------")
    
    main(sys.argv[1:])
    #main('FULL')
    #main('NETCHANGE')
    
    logging.info("------------------    Script End      ------------------")
