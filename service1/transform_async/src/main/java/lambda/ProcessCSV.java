package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import saaf.Inspector;
import saaf.Response;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    /**
     * Lambda Function Handler
     * 
     * @param request 
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        //lambda logger
        LambdaLogger logger = context.getLogger();
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        // select csv file from s3 based on bucketname and filename
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        logger.log("finding file in bucketname:" + bucketname + ", filename:" + filename);
        String aggregation = request.getAggregation();
        String filter = request.getFilter();
        logger.log("aggregation:" + aggregation + ", filter:" + filter);
        //create new file on s3
        
        //get object file using bucket name and filename
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
      
        HashSet<Integer> set = new HashSet<Integer>();
       // read csv file content line by line
       // write the transformed content to a new file on s3
        StringWriter sw = new StringWriter();
        Scanner scanner = new Scanner(objectData); 

        HashSet<String> orderID = new HashSet<>();
        // read the headers
        String headerLine = scanner.nextLine();
        //convert to list to find the index of required columns
        List<String> header = Arrays.asList(headerLine.split(","));
        // find  the index of an element in the array header
        int orderPriorityIndex = header.indexOf("Order Priority");
        int orderIdIndex = header.indexOf("Order ID");
        int shipTimeIndex = header.indexOf("Ship Date");
        int orderTimeIndex = header.indexOf("Order Date");
        int totalProfitIndex = header.indexOf("Total Profit");
        int totalRevenueIndex = header.indexOf("Total Revenue");
        
        // add 'order processing time' column to the header, and write to the new file
        headerLine += ",Order Processing Time,Gross Margin\n";
        sw.append(headerLine);
        while (scanner.hasNext()) {
            //String[] lineText = scanner.nextLine().split(",");
            String line = scanner.nextLine();
            String[] rowData = line.split(",");
            // add 'order processing time' column
            // create a date object for 'ship date' 
            
            try {
                Date shipDate = new SimpleDateFormat("MM/dd/yyyy").parse(rowData[shipTimeIndex]);
                Date orderDate = new SimpleDateFormat("MM/dd/yyyy").parse(rowData[orderTimeIndex]);
                long diff = shipDate.getTime() - orderDate.getTime();
                long diffDays = diff / (24 * 60 * 60 * 1000);
                rowData = Arrays.copyOf(rowData, rowData.length + 1);
                rowData[rowData.length - 1] = String.valueOf(diffDays);
            } catch (ParseException e) {
                Logger.getLogger(ProcessCSV.class.getName()).log(Level.SEVERE, null, e);
            }
            

            // transform 'order priority' from letters to words
            String orderPriority = rowData[orderPriorityIndex];
            switch (orderPriority) {
                case "L":
                    orderPriority = "Low";
                    break;
                case "M":
                    orderPriority = "Medium";
                    break;
                case "H":
                    orderPriority = "High";
                    break;
                case "C":
                    orderPriority = "Critical";
                    break;
            }
            rowData[orderPriorityIndex] = orderPriority;
            // calculate 'gross margin' column
            double totalProfit = Double.parseDouble(rowData[totalProfitIndex]);
            double totalRevenue = Double.parseDouble(rowData[totalRevenueIndex]);
            double grossMargin = totalProfit / totalRevenue;
            rowData = Arrays.copyOf(rowData, rowData.length + 1);
            rowData[rowData.length - 1] = String.format("%.2f",grossMargin);
            // remove duplicate row based on the 'OrderID' column by checking the orderID hashset
            if (orderID.contains(rowData[orderIdIndex])) {
                continue;
            } else {
                orderID.add(rowData[orderIdIndex]);
                
            }
            String newLine = String.join(",", rowData);
            newLine += "\n";
            sw.append(newLine);
            // code to perform the necessary transformation
            
        }
        scanner.close();
        // write the transformed content to a new file on s3
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8); 
        InputStream is = new ByteArrayInputStream(bytes); 
        ObjectMetadata meta = new ObjectMetadata(); 
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        //create transformed csv file on s3
        String transformedFile = "transformed-" + filename;
        String outputBucketname = "transformed-csv";
        s3Client.putObject(outputBucketname, transformedFile, is, meta);
        
        logger.log("ProcessCSV in bucketname:" + outputBucketname + ", filename after transformaiton:" + transformedFile );
        
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl("https://sqs.us-east-2.amazonaws.com/338749838656/562ProjectQueue").withMessageBody("{\"transformedBucketName\":\"" + outputBucketname + "\",\"transformedFileName\":\"" + transformedFile+ "\",\"aggregation\":\"" + aggregation+"\",\"filter\":\""+filter+"\"}").withDelaySeconds(0);
        sqs.sendMessage(send_msg_request);
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: "+ outputBucketname + " filename: " + transformedFile +  " is generated.");
        
        inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        logger.log("Service1 SQS: " + inspector.finish().toString());
        return inspector.finish();
    }
}
