#!/bin/bash  

# JSON object to pass to Lambda Function
json={"\"databaseName\"":"\"MAIN\"","\"tableName\"":"\"orders100\"","\"bucketName\"":"\"transformed-csv\"","\"fileName\"":"\"transformed-100_Sales_Records.csv\""}

echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://h1x4psdcia.execute-api.us-east-2.amazonaws.com/load`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""



# echo "Invoking Lambda function using AWS CLI"
# time output=`aws lambda invoke --invocation-type RequestResponse --function-name AuroraRDS --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""
