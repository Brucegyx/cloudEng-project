#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"test.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://rvhiz7dt8e.execute-api.us-east-2.amazonaws.com/create_csv`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

