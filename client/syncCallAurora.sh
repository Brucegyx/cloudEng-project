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
# json={"\"outputBucketName\"":\"transformed-csv\"","\"transformedFileName\"":\"$transformedFile\""}
json={"\"databaseName\"":"\"MAIN\"","\"tableName\"":"\"orders100\"","\"bucketName\"":"\"transformed-csv\"","\"fileName\"":"\"transformed-100_Sales_Records.csv\""}

echo "Invoking Service 2 using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://h1x4psdcia.execute-api.us-east-2.amazonaws.com/load`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

#### calling service 3 ######

json={"\"databaseName\"":"\"MAIN\"","\"tableName\"":"\"orders100\"","\"aggregation\"":"\"MAX(UnitsSold)\"","\"filter\"":"\"\""}
echo "Invoking Service 3 using API Gateway"
echo $json | jq
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://8pgp3z1mr7.execute-api.us-east-2.amazonaws.com/query`

echo "Service 3 JSON RESULT:"
echo $output | jq



