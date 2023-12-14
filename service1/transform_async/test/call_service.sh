#!/bin/bash
# JSON object to pass to Lambda Function
# json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"test.csv\""}
# time output=`aws lambda invoke --invocation-type Event --function-name Service1_sqs --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

# echo ""
# echo "JSON RESULT:"
# echo $output | jq
# echo ""
aggregation="AVG(GrossMargin)"
filter="Region='Australia and Oceania'"
# Call service 1 lambda function with the target bucket name and file name to transform 
json={"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"$1\"","\"aggregation\"":"\"$aggregation\"","\"filter\"":"\"$filter\""}

echo "Invoking service 1 using API Gateway"
echo $json
time output=`curl -s -H "Content-Type: application/json" -X POST -d "$json" https://ihw6ty6dv2.execute-api.us-east-2.amazonaws.com/TransformAsync`

#time output=`aws lambda invoke --invocation-type Event --function-name Service1_sqs --region us-east-2 /dev/stdout | head -n 1 | head -c -2 ; echo`
#time output=`aws lambda invoke --invocation-type Event --function-name Service1_sqs --region us-east-2 --paylod "$json" response.json`
echo "sync service1 call:"
echo $output | jq
resultFile=${aggregation}_${filter}_"asyncResult.json"
echo $resultFile
if $(aws s3api wait object-exists --bucket 562project-query-async --key "$resultFile")
then
    echo "Result file exists"
    aws s3 cp "s3://562project-query-async/$resultFile" "$resultFile"
    echo "Results copied"
    echo $resultFile | jq -r '.[]'
else
    echo "Result file does not exist"
fi

