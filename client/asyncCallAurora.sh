#!/bin/bash
#Load File to s3 bucket. NOTE: terminal which this is run in must have IAM user
# setup as well as having access key updated for CLI
echo "Bucket before upload"
aws s3 ls s3://test.bucket.462562f23.yx
echo "Name of file being uploaded"
echo $1
aws s3 cp ../data/$1 s3://test.bucket.462562f23.yx/
echo "Bucket after upload"
aws s3 ls s3://test.bucket.462562f23.yx

# set query aggregation function and filter conditions
aggregation="AVG(GrossMargin)"
filter="Region='Australia and Oceania'"
databaseName = "MAIN"
tableName = "async"
# Call service 1 lambda function with the target bucket name, file name to transform, and the aggregation function and filter conditions
json={"\"bucketname\"":\"test.bucket.462562f23.yx\"","\"filename\"":\"$1\"","\"aggregation\"":"\"$aggregation\"","\"filter\"":"\"$filter\"","\"databaseName\"":"\"$databaseName\"","\"tableName\"":"\"$tableName\""}

echo "Invoking service 1 asynchronously using API Gateway"
echo $json
time output=`curl -s -H "Content-Type: application/json" -X POST -d "$json" https://hd6v6ispw2.execute-api.us-east-2.amazonaws.com/AuroraAsync/` 


# echo "async service1 call:"
# echo $output | jq
# resultFile=${aggregation}_${filter}_"asyncResult.json"
# echo $resultFile
# # check periodically if the result file exists, NOTICE: this may return immediately if the same query has been executed before
# if $(aws s3api wait object-exists --bucket 562project-query-async --key "$resultFile")
# then
#     echo "Result file exists"
#     # copy the json file back to the client, this could costs time so we can only check if the result exists
#     aws s3 cp "s3://562project-query-async/$resultFile" "./async_results_$resultFile"
#     echo "Results copied"
#     echo $resultFile | jq -r '.[]'
# else
#     echo "Result file does not exist"
# fi
