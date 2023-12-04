package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class CreateCSV implements RequestHandler<Request, HashMap<String, Object>> {

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
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        int row = request.getRow();
        int col = request.getCol();

        int val = 0;
        StringWriter sw = new StringWriter();
        Random rand = new Random();
        for (int i=0;i<row;i++) {
            for (int j=0;j<col;j++)
            {
                val = rand.nextInt(1000); 
                sw.append(Integer.toString(val)); 
                if ((j+1)!=col)
                    sw.append(",");
                else
                    sw.append("\n");
            }
        }
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8); 
        InputStream is = new ByteArrayInputStream(bytes); 
        ObjectMetadata meta = new ObjectMetadata(); 
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        //create new file on s3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, filename, is, meta);
        
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: "+ bucketname + " filename: " + filename + " size:"  + bytes.length);
        
        inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        return inspector.finish();
    }
}
