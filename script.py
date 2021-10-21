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

URL_YOUMAIL_API_LIST = "https://dataapi.youmail.com/directory/spammers/v2/partial/since/20210109T150000Z/list"
URL_YOUMAIL_API_PARTIAL_HOUR = "https://dataapi.youmail.com/api/v3/spammerlist/partial/"


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
def get_youmail_partial_list(datetime):
    
    try:
        headers = get_youmail_api_headers()
        response = requests.get(URL_YOUMAIL_API_PARTIAL_HOUR + datetime, headers=headers)
    
    except requests.exceptions.RequestException as e:
        print(e)
        raise SystemExit(e)
        
    return response.json()

get_youmail_partial_list("20211020T160000Z")
