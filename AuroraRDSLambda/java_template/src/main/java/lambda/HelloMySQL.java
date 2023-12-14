package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
// import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;

import saaf.Inspector;
import java.util.HashMap;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloMySQL implements RequestHandler<Request, HashMap<String, Object>> {

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
        //Create and populate a separate response object for function output. (OPTIONAL)

        Response r = new Response();

        try 
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            String dbName = "TEST";
            String tableName = "ordertest";

            url += "/"+dbName; // for test
            
            Connection con = DriverManager.getConnection(url,username,password);
            Statement stmt = con.createStatement();

            // try {
            //     stmt.executeUpdate("DROP DATABASE IF EXISTS " + dbName);
            // } catch (Exception e) {
            //     logger.log("Failed to drop DB: " + e.getMessage());
            // }

            // logger.log(dbName + " Dropped");

            // String sql_createDB = "CREATE DATABASE IF NOT EXISTS " + dbName;
            // stmt.executeUpdate(sql_createDB);
            // // stmt.executeUpdate("USE " + dbName);

            // logger.log(dbName + " created");

            // url += "/"+dbName;
            // con = DriverManager.getConnection(url,username,password);


            try {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            } catch (Exception e) {
                logger.log("Failed to drop table: " + e.getMessage());
            }

            logger.log(tableName + " Dropped");


            String sql_createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` ("
                        + "`OrderID` VARCHAR(255) PRIMARY KEY, "
                        + "`Region` VARCHAR(255), "
                        + "`Country` VARCHAR(255), "
                        + "`ItemType` VARCHAR(255), "
                        + "`SalesChannel` VARCHAR(255), "
                        + "`OrderPriority` VARCHAR(255), "
                        + "`OrderDate` VARCHAR(255), "
                        + "`ShipDate` VARCHAR(255), "
                        + "`UnitsSold` INT, "
                        + "`UnitPrice` DECIMAL(16, 6), "
                        + "`UnitCost` DECIMAL(16, 6), "
                        + "`TotalRevenue` DECIMAL(16, 6), "
                        + "`TotalCost` DECIMAL(16, 6), "
                        + "`TotalProfit` DECIMAL(16, 6), "
                        + "`OrderProcessingTime` INT, "
                        + "`GrossMargin` DECIMAL(16, 6))";
    
            
            stmt.executeUpdate(sql_createTable);
            logger.log(tableName + " created");

            // String bucket = request.getBucketName();
            // String key = request.getFileName();

            String bucket = "transformed-csv";
            String filename = "transformed-100_Sales_Records.csv";

            
            try {
                // Get the object from the S3 bucket
                // S3Object s3Object = s3Client.getObject(bucket, filename);
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, filename));
                logger.log("got s3Object");

                S3ObjectInputStream s3InputStream = s3Object.getObjectContent();
                logger.log("got s3InputStream");

                // // get object file using bucket name and filename
                // S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, filename));
                // // get content of the file
                // InputStream objectData = s3Object.getObjectContent();

                String insertDataQuery = "INSERT INTO "+ tableName +" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement prepStmt = con.prepareStatement(insertDataQuery);

                logger.log("Read CSV Step 1 done");

                // Process the CSV file line by line
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {

                    // Skip the first line (header)
                    reader.readLine();

                    String line;
                    logger.log("Read CSV Step 2 done");

                    int lineNum = 1;
                    while ((line = reader.readLine()) != null) {
                        // Process each line of the CSV file
                        String[] fields = line.split(",");
                        logger.log("split line " + lineNum);

                        prepStmt.setString(1, fields[6]); // OrderID (Primary Key)
                        prepStmt.setString(2, fields[0]); // Region
                        prepStmt.setString(3, fields[1]); // Country
                        prepStmt.setString(4, fields[2]); // ItemType
                        prepStmt.setString(5, fields[3]); // SalesChannel
                        prepStmt.setString(6, fields[4]); // OrderPriority
                        prepStmt.setString(7, fields[5]); // OrderDate
                        prepStmt.setString(8, fields[7]); // ShipDate
                        prepStmt.setInt(9, Integer.parseInt(fields[8]));        // UnitsSold
                        prepStmt.setDouble(10, Double.parseDouble(fields[9]));  // UnitPrice
                        prepStmt.setDouble(11, Double.parseDouble(fields[10])); // UnitCost
                        prepStmt.setDouble(12, Double.parseDouble(fields[11])); // TotalRevenue
                        prepStmt.setDouble(13, Double.parseDouble(fields[12])); // TotalCost
                        prepStmt.setDouble(14, Double.parseDouble(fields[13])); // TotalProfit
                        prepStmt.setInt(15, Integer.parseInt(fields[14]));      // OrderProcessingTime
                        prepStmt.setDouble(16, Double.parseDouble(fields[15])); // GrossMargin
                        // Set prepared statement fields based on CSV fields
                        prepStmt.executeUpdate();

                        logger.log("Read CSV line: " + lineNum++);

                    }
                } catch(Exception e) {
                    logger.log("Error reading csv file: " + e.getMessage());
                }

                // try (Scanner scanner = new Scanner(objectData)) {
                //     PreparedStatement preparedStatement = con.prepareStatement(insertDataQuery);
                //     scanner.nextLine(); // Skip the header line

                //     int lineNum = 1;
                //     while (scanner.hasNext()) {
                //         String[] fields = scanner.nextLine().split(",");
                //         preparedStatement.setString(1, fields[6]); // OrderID (Primary Key)
                //         preparedStatement.setString(2, fields[0]); // Region
                //         preparedStatement.setString(3, fields[1]); // Country
                //         preparedStatement.setString(4, fields[2]); // ItemType
                //         preparedStatement.setString(5, fields[3]); // SalesChannel
                //         preparedStatement.setString(6, fields[4]); // OrderPriority
                //         preparedStatement.setString(7, fields[5]); // OrderDate
                //         preparedStatement.setString(8, fields[7]); // ShipDate
                //         preparedStatement.setInt(9, Integer.parseInt(fields[8])); // UnitsSold
                //         preparedStatement.setDouble(10, Double.parseDouble(fields[9])); // UnitPrice
                //         preparedStatement.setDouble(11, Double.parseDouble(fields[10])); // UnitCost
                //         preparedStatement.setDouble(12, Double.parseDouble(fields[11])); // TotalRevenue
                //         preparedStatement.setDouble(13, Double.parseDouble(fields[12])); // TotalCost
                //         preparedStatement.setDouble(14, Double.parseDouble(fields[13])); // TotalProfit
                //         preparedStatement.setInt(15, Integer.parseInt(fields[14])); // OrderProcessingTime
                //         preparedStatement.setDouble(16, Double.parseDouble(fields[15])); // GrossMargin
                //         // Set prepared statement fields based on CSV fields
                //         preparedStatement.executeUpdate();

                //         logger.log("Read CSV line: " + lineNum++);
                //     }
                // } catch (Exception e) {
                //     logger.log("Error reading csv file: " + e.getMessage());
                // }

                // logger.log(" CSV inserted into DB");


            } catch (Exception e) {
                logger.log("Error processing S3 object: " + e.getMessage());
            }


            // // retrieves only the first 10 rows of table
            // String selectSql = "SELECT * FROM " + tableName + " LIMIT 10";
            // try (Statement selectStmt = con.createStatement();
            //      ResultSet rs = selectStmt.executeQuery(selectSql)) {
                
            //     int row = 1;
            
            //     while (rs.next()) {
            //         // Retrieve each column value
            //         String c1 = rs.getString("OrderID");
            //         String c2 = rs.getString("Region");
            //         String c3 = rs.getString("Country");
            //         String c4 = rs.getString("ItemType");
            //         String c5 = rs.getString("SalesChannel");
            //         String c6 = rs.getString("OrderPriority");
            //         String c7 = rs.getString("OrderDate");
            //         String c8 = rs.getString("ShipDate");
            //         int c9 = rs.getInt("UnitsSold");
            //         double c10 = rs.getDouble("UnitPrice");
            //         double c11 = rs.getDouble("UnitCost");
            //         double c12 = rs.getDouble("TotalRevenue");
            //         double c13 = rs.getDouble("TotalCost");
            //         double c14 = rs.getDouble("TotalProfit");
            //         int c15 = rs.getInt("OrderProcessingTime");
            //         double c16 = rs.getDouble("GrossMargin");


            //         logger.log("Row"+ row +": "+ c1 +", "+ c2 +", "+ c3 +", "+ c4 +", "+ c5 +", "+ 
            //                 c6 +", "+ c7 +", "+ c8 +", "+ c9 +", "+ c10 +", "+ c11 +", "+ c12 +", "+ 
            //                 c13 +", "+ c14 +", "+ c15 +", "+ c16);

            //     }
            // } catch (SQLException e) {
            //     logger.log("SQL Exception while selecting data: " + e.getMessage());
            // } catch (Exception e) {
            //     logger.log("Got an exception working with MySQL! ");
            //     logger.log(e.getMessage());
            // }

            con.close();


            // ************* OLD ********************** //
            // r.setValue(request.getName());
            // PreparedStatement ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','b','c');");
            // ps.execute();
            // PreparedStatement ps = con.prepareStatement("select * from mytable;");
            // ResultSet rs = ps.executeQuery();
            // LinkedList<String> ll = new LinkedList<String>();
            // while (rs.next())
            // {
            //     logger.log("name=" + rs.getString("name"));
            //     ll.add(rs.getString("name"));
            //     logger.log("col2=" + rs.getString("col2"));
            //     logger.log("col3=" + rs.getString("col3"));
            // }
            // rs.close();

            // //query to get version of MySQL
            // ps = con.prepareStatement("select version() as version;");
            // rs = ps.executeQuery();
            // String v = new String();
            // while(rs.next()){
            //     logger.log("version=" + rs.getString("version"));
            //     v = rs.getString("version");
            // }
            // rs.close();
            // con.close();
            // r.setMySQLVersion(v);
            // r.setNames(ll);


        } 
        catch (Exception e) 
        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        //Print log information to the Lambda log as needed
        //logger.log("log message...");
        
        inspector.consumeResponse(r);
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    // private boolean getS3Data(String bucket, String key) {
    //     AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    //     try {
    //         // Get the object from the S3 bucket
    //         S3Object s3Object = s3Client.getObject(bucket, key);
    //         S3ObjectInputStream s3InputStream = s3Object.getObjectContent();

    //         // Process the CSV file line by line
    //         try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {
    //             String line;
    //             while ((line = reader.readLine()) != null) {
    //                 // Process each line of the CSV file
    //             }
    //         }
    //     } catch (Exception e) {
    //         logger.log("Error processing S3 object: " + e.getMessage());
    //         return false;
    //     }
    //     return true;
    // }


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
        HelloMySQL lt = new HelloMySQL();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getName());

        // Test properties file creation
        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url","");
        properties.setProperty("username","");
        properties.setProperty("password","");
        try
        {
          properties.store(new FileOutputStream("test.properties"),"");
        }
        catch (IOException ioe)
        {
          System.out.println("error creating properties file.")   ;
        }


        // Run the function
        //Response resp = lt.handleRequest(req, c);
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
        Response resp = new Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

}
