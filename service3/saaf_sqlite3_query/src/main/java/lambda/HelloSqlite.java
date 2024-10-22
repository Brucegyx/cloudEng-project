package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


import org.json.JSONObject;
import org.json.JSONArray;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloSqlite implements RequestHandler<Request, HashMap<String, Object>> {
    static int uses = 0;
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        logger.log(request.toString());
        String dbBucket = request.getDbBucket();
        String dbFile = request.getDbFile();
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
        // Check if the required db file is in the dbBucket
        
        
        // if not, return error message

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
            String filter = request.getFilter();
            String aggregation = request.getAggregation();
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
            /* 
            // Load query results for [name] column into a Java Linked List
            // ignore [col2] and [col3] 
            LinkedList<String> ll = new LinkedList<String>();
            while (rs.next())
            {
                logger.log("name=" + rs.getString("name"));
                ll.add(rs.getString("name"));
                logger.log("col2=" + rs.getString("col2"));
                logger.log("col3=" + rs.getString("col3"));
            }*/
            rs.close();
            con.close();  
            System.out.println("result=" + result.toString());
            r.setValue(result.toString()); // set the array of JSON objects as the query result in the response
            
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
        /* 
        // *********************************************************************
        // Set hello message here
        // *********************************************************************
        uses += 1;
        String hello = "Hello This is sqlite3 query" + request.getName() + " calls: " + uses ;

        // Set return result in Response class, class is marshalled into JSON
        r.setValue(hello);
                // Test code for this to work -------------
        LinkedList<String> names = new LinkedList<String>();
        names.add("name1");
        r.setNames(names);
        // Test code for this to work -----------------
        */
        inspector.consumeResponse(r);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
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
    public static void main (String[] args)
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

    
}
