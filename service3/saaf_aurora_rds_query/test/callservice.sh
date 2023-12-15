#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"databaseName\"":"\"MAIN\"","\"tableName\"":"\"orders100\"","\"aggregation\"":"\"MAX(UnitsSold)\"","\"filter\"":"\"\""}

#echo "Invoking Lambda function using API Gateway"
#time output=`curl -s -H "Content-Type: application/json" -X POST -d $json {API-GATEWAY-REST-URL}`
#echo ""

#echo ""
#echo "JSON RESULT:"
#echo $output | jq
#echo ""

echo "Invoking Lambda function using AWS CLI"
#time output=`aws lambda invoke --invocation-type RequestResponse --function-name {LAMBDA-FUNCTION-NAME} --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
time output=`aws lambda invoke --invocation-type RequestResponse --function-name service3_aurora --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
