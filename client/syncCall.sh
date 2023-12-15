#!/bin/bash
# JSON object to pass to Lambda Function
# json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"test.csv\""}
# echo "Invoking Lambda function using API Gateway"
# time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://l8ges5uu0h.execute-api.us-east-2.amazonaws.com/Transform`
# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""



#Load File to s3 bucket. NOTE: terminal which this is run in must have IAM user
# setup as well as having access key updated for CLI
echo "Bucket before upload"
aws s3 ls s3://test.bucket.462562f23.yx
echo "Name of file being uploaded"
echo $1
aws s3 cp ../data/$1 s3://test.bucket.462562f23.yx/
echo "Bucket after upload"
aws s3 ls s3://test.bucket.462562f23.yx


# Call service 1 lambda function with the target bucket name and file name to transform 
json={"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"$1\""}

echo "Invoking service 1 using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://l8ges5uu0h.execute-api.us-east-2.amazonaws.com/Transform/`

echo "Service 1 JSON RESULT:"
echo $output | jq

echo "Transform Bucket"
aws s3 ls s3://transformed-csv

###### calling service 2 ######
# JSON object to pass to Lambda Function
transformedFile="transformed-$1"
echo $transformedFile
json={"\"outputBucketName\"":\"transformed-csv\"","\"transformedFileName\"":\"$transformedFile\""}

echo "Invoking Service 2 using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://v8le7b4h00.execute-api.us-east-2.amazonaws.com/Load`

echo "Service 2 JSON RESULT:"
echo $output | jq
echo ""


#### calling service 3 ######
filenameWithoutSuffix=${1%.csv}   # remove suffix from filename
dbFile="transformed-$filenameWithoutSuffix.db"
echo $dbFile
json={"\"dbBucket\"":"\"sqlite-db562\"","\"dbFile\"":"\"$dbFile\"","\"aggregation\"":"\"AVG(GrossMargin) \"","\"filter\"":"\"\""}
echo "Invoking Service 3 using API Gateway"
echo $json | jq
time output=`curl -s -H "Content-Type: application/json" -X POST -d "$json" https://db4m5x9c3a.execute-api.us-east-2.amazonaws.com/query`
#time output=`aws lambda invoke --invocation-type RequestResponse --function-name Service3 --region us-east-2 --payload "$json" /dev/stdout | head -n 1 | head -c -2 ; echo`

echo "Service 3 JSON RESULT:"
echo $output | jq



