# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import json
import requests
import pandas as pd
import csv
from datetime import datetime

# +
URL_YOUMAIL_API_LIST = "https://dataapi.youmail.com/directory/spammers/v2/partial/since/"
URL_YOUMAIL_API_FULL = "https://dataapi.youmail.com/api/v3/spammerlist/full"
URL_YOUMAIL_API_PARTIAL_HOUR = "https://dataapi.youmail.com/api/v3/spammerlist/partial/"

CSV_FOLDER = "files"
YOUMAIL_FULL_FILENAME = "spam-number-file.csv"
YOUMAIL_PART_FILENAME = "spam-number-file_"


# -

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
        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_PARTIAL_HOUR + datetime, headers=headers)
    
    except requests.exceptions.RequestException as e:
        print(e)
        raise SystemExit(e)
        
    return response.json()

#hourly = get_youmail_partial_list("20211020T160000Z")


# -

#get full spam list
def get_youmail_full_list():
    
    try:
        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_FULL, headers=headers)
    
    except requests.exceptions.RequestException as e:
        print(e)
        raise SystemExit(e)
        
    return response.json()


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
        df.to_csv(CSV_FOLDER + "/" + YOUMAIL_FULL_FILENAME, index=False)
    
    except Exception as e:
        print(e)
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


#get the delta 
def save_this_hour_partial_spam_list():
    
    base = datetime.today()
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
    
    #calculate Operation field based on last full list
    df['Operation'] = df.apply(lambda row: partial_number_operation(row['Number'], 
                                                                    row['SpamScore'], 
                                                                    row['FraudProbability'], 
                                                                    row['TCPAFraudProbability']), axis=1)
    
    #spam-number-file_yyyymmddhh:mm:ss
    time_file = base.strftime('%Y%m%d%H:00:00')
    
    df.to_csv(CSV_FOLDER + "/" + YOUMAIL_PART_FILENAME + hour + ".csv", index=False)
    
    return


save_this_hour_partial_spam_list()
