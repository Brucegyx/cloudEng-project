package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import org.json.JSONObject;
import org.json.JSONArray;
import com.google.gson.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloSqlite implements RequestHandler<SQSEvent, HashMap<String, Object>> {
    static int uses = 0;
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(SQSEvent sqsEvent, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        try {
            for (SQSEvent.SQSMessage msg : sqsEvent.getRecords()){
                String body = msg.getBody();
                logger.log("body=" + body);
                Gson gson = new Gson();
                SqsMessages sqsMsg = gson.fromJson(body, SqsMessages.class);
                String dbBucket = sqsMsg.getDbBucketName();
                String dbFile = sqsMsg.getDbFile();

                logger.log("dbBucket=" + dbBucket);
                logger.log("dbFile=" + dbFile);

                //Create and populate a separate response object for function output. (OPTIONAL)
                Response r = new Response();
                
                String pwd = System.getProperty("user.dir");
                logger.log("pwd=" + pwd);

                logger.log("set pwd to tmp");        
                setCurrentDirectory("/tmp");
                
                pwd = System.getProperty("user.dir");
                logger.log("pwd=" + pwd);
                //Check if the db file exists in /tmp
                
                File db = new File("/tmp/"+dbFile);
                logger.log(db.getAbsolutePath());
                if (db.exists() &&  db.isFile()) {
                    logger.log("db file exists in /tmp, using local copy of db file");
                } else {
                    logger.log("db file does not exist in /tmp, downloading from bucket");
                    try {
                        // download it to /tmp if file exists
                        // Check if the required db file is in the dbBucket if not, throw exception
                        GetObjectRequest getObjectRequest = new GetObjectRequest(dbBucket, dbFile);
                        s3Client.getObject(getObjectRequest,  db);
                        logger.log("downloaded db file from bucket");
                    } catch (Exception e) {
                        logger.log("db file does not exist in bucket, exception: "  + e.toString());
                        r.setValue("db file does not exist in bucket");
                        inspector.consumeResponse(r);
                        return inspector.finish();
                    }
                }
                

                // UNCOMMENT THIS SECTION TO USE SQLITE DB 
                try
                {
                    // Connection string for a file-based SQlite DB
                    Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/" + dbFile);

                    // Detect if the table 'mytable' exists in the database
                    PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'");
                    ResultSet rs = ps.executeQuery();
                    if (!rs.next())
                    {
                        // 'mytable' does not exist, error must happen in Service 2
                        logger.log("no table found, error must happen in Service 2");
                        r.setValue("no table found, error must happen in Service 2");
                        rs.close();
                        con.close();
                        inspector.consumeResponse(r);
                        return inspector.finish();
                    }
                    rs.close();

                
                    

                    // Query mytable to obtain full resultset
                    // perform client query 
                    String filter = sqsMsg.getFilter();
                    String aggregation = sqsMsg.getAggregation();
                    String query = "";
                    if (filter == null || filter.equals("") || filter.equals("*")) {
                        query = "SELECT " + aggregation + " FROM orders";
                    } else { 
                        query = "SELECT " + aggregation + " FROM orders WHERE " + filter;
                    }
                    ps = con.prepareStatement(query);
                    rs = ps.executeQuery();
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    List<String> columnNames = new ArrayList<String>();
                    for (int i = 1; i <= columnsNumber; i++) {
                        columnNames.add(rsmd.getColumnName(i));
                    }
                    JSONArray result = new JSONArray();
                    while (rs.next()) {
                        JSONObject obj = new JSONObject();
                        for ( String column : columnNames) {
                            try {

                                obj.put(column, rs.getString(column));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                        
                        result.put(obj);
                    }
  
                    rs.close();
                    con.close(); 
                    String inputFileWithoutSuffix = dbFile.substring(0, dbFile.lastIndexOf('.')); 
                    String resultFilename = inputFileWithoutSuffix+"_"+aggregation+"_"+filter+"_asyncResult.json";
                    
                    r.setValue(result.toString()); // set the array of JSON objects as the query result in the response
                    s3Client.putObject("562project-query-async", resultFilename, result.toString());
                    // sleep to ensure that concurrent calls obtain separate Lambdas
                    try
                    {
                        Thread.sleep(200);
                    }
                    catch (InterruptedException ie)
                    {
                        logger.log("interrupted while sleeping...");
                    }
                }
                catch (SQLException sqle)
                {
                    logger.log("DB ERROR:" + sqle.toString());
                    sqle.printStackTrace();
                }
                
                inspector.consumeResponse(r);
            }
            //****************END FUNCTION IMPLEMENTATION***************************
            
            //Collect final information such as total runtime and cpu deltas.
            inspector.inspectAllDeltas();
            logger.log(inspector.finish().toString());
            return inspector.finish();

        } catch (Exception e) {
            logger.log("Exception:" + e.toString());
            e.printStackTrace();
            
        }
        inspector.inspectAllDeltas();
        logger.log(inspector.finish().toString());
        return inspector.finish();
       
    }
    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }


    // int main enables testing function from cmd line
/*     public static void main (String[] args)
   {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        // Create an instance of the class
        HelloSqlite lt = new HelloSqlite();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getName());

        // Run the function
        HashMap resp = lt.handleRequest(req, c);
        try
        {
            Thread.sleep(10);
        }
        catch (InterruptedException ie)
        {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
*/
    
}
