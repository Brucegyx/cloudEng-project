#!/bin/bash

#### calling service 3 ######
filenameWithoutSuffix=${1%.csv}   # remove suffix from filename
dbFile="transformed-$filenameWithoutSuffix.db"
echo $dbFile
json={"\"dbBucket\"":"\"sqlite-db562\"","\"dbFile\"":"\"$dbFile\"","\"aggregation\"":"\"AVG(GrossMargin) \"","\"filter\"":"\"\""}
echo "Invoking Service 3 using API Gateway"
echo $json | jq
#time output=`curl -s -H "Content-Type: application/json" -X POST -d "$json" https://db4m5x9c3a.execute-api.us-east-2.amazonaws.com/query`
time output=`aws lambda invoke --invocation-type RequestResponse --function-name Service3 --region us-east-2 --payload "$json" /dev/stdout | head -n 1 | head -c -2 ; echo`

echo "Service 3 JSON RESULT:"
echo $output | jq
