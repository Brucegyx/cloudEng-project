#!/bin/bash
# JSON object to pass to Lambda Function
# json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"test.csv\""}
# echo "Invoking Lambda function using API Gateway"
# time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://l8ges5uu0h.execute-api.us-east-2.amazonaws.com/Transform`
# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""
###### calling service 2 ######
# JSON object to pass to Lambda Function
json={"\"name\"":"\"Susan\u0020Smith\",\"param1\"":1,\"param2\"":2,\"param3\"":3}

echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://v8le7b4h00.execute-api.us-east-2.amazonaws.com/Load`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

# echo "Invoking Lambda function using AWS CLI"
# time output=`aws lambda invoke --invocation-type RequestResponse --function-name Service2 --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""