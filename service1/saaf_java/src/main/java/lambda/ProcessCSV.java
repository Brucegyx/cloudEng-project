package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.stream.Stream;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        // select csv file from s3 based on bucketname and filename
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        int row = request.getRow();
        int col = request.getCol();

        //create new file on s3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using bucket name and filename
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
      
        HashSet<Integer> set = new HashSet<Integer>();
       // read csv file content line by line
       // write the transformed content to a new file on s3
        StringWriter sw = new StringWriter();
        Scanner scanner = new Scanner(objectData); 
        while (scanner.hasNext()) {
            //String[] lineText = scanner.nextLine().split(",");
            sw.append(scanner.nextLine() + "\n");
            // code to perform the necessary transformation
            
        }
        scanner.close();
        

        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8); 
        InputStream is = new ByteArrayInputStream(bytes); 
        ObjectMetadata meta = new ObjectMetadata(); 
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        //create transformed csv file on s3
        String transformedFile = "transformed-" + filename;
        String outputBucketname = "transformed-csv";
        s3Client.putObject(outputBucketname, transformedFile, is, meta);
        LambdaLogger logger = context.getLogger();
        logger.log("ProcessCSV in bucketname:" + outputBucketname + ", filename after transformaiton:" + transformedFile );
        
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: "+ outputBucketname + " filename: " + transformedFile +  " is generated.");
        
        inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        return inspector.finish();
    }
}
