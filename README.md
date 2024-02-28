# youmail-to-s3
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

## Cron 
Run FULL sync daylly (cat /etc/cron.d/full-youmail)
```
0 0 * * * root python3 /var/apps/s3-sync-script/script.py FULL
```
Run NETCHANGE sync hourly (/etc/cron.d/changelog-youmail)
```
0 * * * * root python3 /var/apps/s3-sync-script/script.py NETCHANGE
```
Run CLEAN UP obsolete files (/etc/cron.d/clean-files-youmail)
```
30 0 * * * root python3 /var/apps/s3-sync-script/script.py CLEAN
```
