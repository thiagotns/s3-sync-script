#!/bin/bash

data=`/bin/date +%Y%m%d-%H%M%S`

python3 script.py $1 > log/s3-sync-youmail-$data.log