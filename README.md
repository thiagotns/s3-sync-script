# s3-sync-script
Python script to sync data from Youmail api to S3.

## How To Install 

Install the requirements
```
pip install -r requirements.txt
```

Create a credentials JSON file from credentials.dist.json
```
cp credentials.dist.json credentials.json
```

Setup the credentials 
```
vim credentials.json
```

## How to run

Sync FULL
```
python3 script.py FULL
```

Sync NETCHANGE
```
python3 script.py NETCHANGE
```

## Log Folder
```
s3-sync-script/log
```

##Cron 
Run FULL sync daylly
```
0 0 * * * python3 /home/thiago/s3-sync-script/script.py FULL
```
Run NETCHANGE sync daylly
```
0 * * * * python3 /home/thiago/s3-sync-script/script.py NETCHANGE
```